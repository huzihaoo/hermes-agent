from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_control_store import (
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
)
from gateway.pnc_rca_delivery_store import (
    DeliveryCapacitySnapshot,
    RcaDeliveryStore,
)
from gateway.pnc_rca_capacity_runtime import CapacityRuntimePaths
from gateway.pnc_rca_capacity_sample_evidence import (
    canonical_bytes,
    producer_activation_path,
    write_owner_only_create_once,
)
from scripts import pnc_rca_delivery_collector as collector_module
from scripts.pnc_rca_delivery_collector import (
    ArtifactBundleReadError,
    CollectorConfig,
    DeliveryCollector,
    HealthReporter,
    read_health,
    run_collector_loop,
)
from tests.gateway.test_pnc_rca_delivery_contract import _bundle
from tests.gateway.test_pnc_rca_delivery_store import (
    NOW,
    _control,
    _insert_subscription,
)
from tests.gateway.test_pnc_rca_capacity_sample_evidence import (
    KEY as CAPACITY_KEY,
    RELEASE_ID as CAPACITY_RELEASE_ID,
    _activation as capacity_activation,
    _snapshot as capacity_snapshot,
    _terminal as capacity_terminal,
)
from tests.gateway.test_pnc_rca_capacity_transition import bootstrap_state


def _config(
    tmp_path,
    *,
    enabled: bool = True,
    grace_seconds: int = 900,
    capacity_sample_enabled: bool = False,
):
    return CollectorConfig.from_env(
        {
            "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": str(enabled).lower(),
            "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
                tmp_path / "control.sqlite3"
            ),
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
            "HERMES_RCA_DELIVERY_COLLECTOR_POLL_INTERVAL_SECONDS": "1",
            "HERMES_RCA_DELIVERY_COLLECTOR_RUNNING_POLL_SECONDS": "20",
            "HERMES_RCA_DELIVERY_COLLECTOR_MAX_POLL_SECONDS": "300",
            "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_COLLECTOR_BATCH_SIZE": "5",
            "HERMES_RCA_DELIVERY_COLLECTOR_BACKFILL_BATCH_SIZE": "100",
            "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_MAX_AGE_SECONDS": "60",
            "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
            "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
            "HERMES_RCA_DELIVERY_COLLECTOR_TERMINAL_ARTIFACT_GRACE_SECONDS": str(
                grace_seconds
            ),
            "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED": str(
                capacity_sample_enabled
            ).lower(),
            "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_BATCH_SIZE": "5",
            "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_LOCK_TIMEOUT_SECONDS": "5",
            "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_TERMINAL_RECEIPT_TIMEOUT_SECONDS": "15",
        },
        hermes_home=tmp_path,
    )


def test_collector_main_disables_dotenv_interpolation(monkeypatch):
    calls = []

    def observe(*args, **kwargs):
        calls.append((args, kwargs))

    def invalid_config(*_args, **_kwargs):
        raise ValueError("stop-after-env-load")

    monkeypatch.setattr(collector_module, "load_dotenv", observe)
    monkeypatch.setattr(collector_module.CollectorConfig, "from_env", invalid_config)

    assert collector_module.main(["--check-config"]) == 2
    assert calls[0][1] == {"override": False, "interpolate": False}


def test_collector_environment_loader_preserves_literal_expansion_syntax(
    tmp_path, monkeypatch
):
    env_file = tmp_path / "collector.env"
    key = "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT"
    env_file.write_text(f"{key}=${{AMBIENT_AGENT}}\n", encoding="utf-8")
    monkeypatch.setenv("AMBIENT_AGENT", "/unexpected/expanded-agent")
    monkeypatch.delenv(key, raising=False)

    try:
        collector_module.load_collector_environment(env_file)
        assert os.environ[key] == "${AMBIENT_AGENT}"
    finally:
        os.environ.pop(key, None)


def test_collector_config_exposes_activation_required(tmp_path):
    config = CollectorConfig.from_env(
        {"HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED": "true"},
        hermes_home=tmp_path,
    )

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "off", ""])
def test_collector_activation_required_rejects_boolean_aliases(tmp_path, value):
    with pytest.raises(ValueError, match="exactly true or false"):
        CollectorConfig.from_env(
            {"HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED": value},
            hermes_home=tmp_path,
        )


def _bundle_payload():
    _admission, contract, manifest, observed, dependencies = _bundle()
    return {
        "delivery_contract": contract,
        "delivery_manifest": manifest,
        "observed_files": observed,
        "html_dependencies": dependencies,
    }


def _collector(
    tmp_path,
    *,
    status_reader=None,
    bundle_reader=None,
    enabled: bool = True,
    now=None,
):
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    return DeliveryCollector(
        store=store,
        config=_config(tmp_path, enabled=enabled),
        status_reader=status_reader
        or (
            lambda task_id: {"success": True, "task_id": task_id, "state": "completed"}
        ),
        artifact_bundle_reader=bundle_reader or (lambda claim: _bundle_payload()),
        now=now or (lambda: NOW),
        lease_owner="collector-test",
    )


def _run_remote_bundle_script(tmp_path, files, artifact_rows):
    submission_key = "g1q3-rca-s1-" + "a" * 64
    root = tmp_path / "bundle"
    root.mkdir(parents=True)
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = content.encode("utf-8") if isinstance(content, str) else content
        path.write_bytes(payload)
    (root / "delivery_contract.json").write_text("{}", encoding="utf-8")
    (root / "delivery_manifest.json").write_text(
        json.dumps({"artifacts": artifact_rows}), encoding="utf-8"
    )
    script = collector_module._remote_bundle_script(submission_key)
    canonical = f"/mnt/tmp/{submission_key}/"
    local = str(root) + "/"
    script = script.replace(f"ROOT = {canonical!r}", f"ROOT = {local!r}", 1)
    process = subprocess.run(
        [sys.executable, "-c", script],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def _artifact_rows(files):
    media_types = {
        ".html": "text/html; charset=utf-8",
        ".css": "text/css; charset=utf-8",
        ".js": "text/javascript; charset=utf-8",
        ".json": "application/json",
        ".png": "image/png",
        ".woff2": "font/woff2",
    }
    return [
        {
            "role": "index_html" if path == "index.html" else f"asset-{index}",
            "path": path,
            "media_type": media_types.get(
                Path(path).suffix, "application/octet-stream"
            ),
        }
        for index, path in enumerate(files)
    ]


def test_config_is_disabled_and_external_write_free_by_default(tmp_path):
    config = CollectorConfig.from_env({}, hermes_home=tmp_path)
    public = config.public_dict()

    assert config.enabled is False
    assert public["external_writes"] is False
    assert public["control_db_path"].endswith("control.sqlite3")


def test_remote_parser_probe_runs_only_the_hash_pinned_canonical_checker(
    monkeypatch,
):
    expected = collector_module.expected_remote_css_runtime_dependency()
    payload = {
        "schema_version": expected["schema_version"],
        "ok": True,
        "mutates_state": False,
        "python": {
            "expected_executable": expected["python_executable"],
            "actual_executable": expected["python_executable"],
            "same_file": True,
        },
        "requirements": {
            "path": expected["requirements_path"],
            "sha256": expected["requirements_sha256"],
            "pins": {
                "tinycss2": expected["version"],
                "webencodings": expected["webencodings_version"],
            },
        },
        "runtime_versions": {
            "tinycss2": expected["version"],
            "webencodings": expected["webencodings_version"],
        },
        "semantic_checks": {
            "escaped_url_tokenized": True,
            "numeric_values_tokenized": True,
            "webencodings_utf8_lookup": True,
        },
        "errors": [],
    }
    captured = {}

    def run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    monkeypatch.setattr(collector_module.subprocess, "run", run)

    assert collector_module.probe_remote_css_parser("/safe/ssh-mini-agent") == expected
    assert captured["command"] == [
        "/safe/ssh-mini-agent",
        "run_bash_json",
    ]
    assert expected["checker_path"] in captured["input"]
    assert expected["checker_sha256"] in captured["input"]
    assert expected["requirements_path"] in captured["input"]
    assert expected["requirements_sha256"] in captured["input"]
    assert "run_py_json" not in captured["input"]


def test_remote_parser_probe_can_verify_bound_worker_candidate(monkeypatch):
    expected = collector_module.expected_remote_css_runtime_dependency()
    worker_root = "/mnt/tmp/release/worker-candidate"
    requirements_path = f"{worker_root}/requirements-rca-delivery.txt"
    payload = {
        "schema_version": expected["schema_version"],
        "ok": True,
        "mutates_state": False,
        "python": {
            "expected_executable": expected["python_executable"],
            "actual_executable": expected["python_executable"],
            "same_file": True,
        },
        "requirements": {
            "path": requirements_path,
            "sha256": expected["requirements_sha256"],
            "pins": {
                "tinycss2": expected["version"],
                "webencodings": expected["webencodings_version"],
            },
        },
        "runtime_versions": {
            "tinycss2": expected["version"],
            "webencodings": expected["webencodings_version"],
        },
        "semantic_checks": {
            "escaped_url_tokenized": True,
            "numeric_values_tokenized": True,
            "webencodings_utf8_lookup": True,
        },
        "errors": [],
    }
    captured = {}

    def run(command, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    monkeypatch.setattr(collector_module.subprocess, "run", run)

    assert collector_module.probe_remote_css_parser(
        "/safe/ssh-mini-agent",
        worker_root=worker_root,
    ) == expected
    assert f"{worker_root}/check_rca_delivery_runtime.py" in captured["input"]
    assert requirements_path in captured["input"]
    assert expected["checker_sha256"] in captured["input"]
    assert expected["requirements_sha256"] in captured["input"]


def test_completed_service_task_is_watched_by_stable_non_timestamp_id(tmp_path):
    _control(tmp_path)
    seen = []
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: (
            seen.append(task_id)
            or {"success": True, "task_id": task_id, "state": "completed"}
        ),
    )

    outcomes = collector.collect_batch()

    assert outcomes[0].status == "delivery_created"
    assert seen == [outcomes[0].submission_key]
    assert seen[0].startswith("g1q3-rca-s1-")
    assert len(collector.store.list_rows("rca_delivery_jobs")) == 1
    effects = collector.store.list_rows("rca_delivery_effects")
    assert len(effects) == 1
    assert effects[0]["effect_kind"] == "feishu_issue_comment"
    assert effects[0]["status"] == "pending"
    assert collector.store.list_rows("rca_delivery_attempts") == []


def test_running_status_is_rescheduled_without_artifact_read(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "running",
        },
        bundle_reader=lambda claim: (_ for _ in ()).throw(
            AssertionError("running task must not read artifacts")
        ),
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "running"
    watch = collector.store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "running"
    assert watch["next_poll_at"] == (NOW + timedelta(seconds=20)).isoformat()
    assert collector.store.list_rows("rca_delivery_jobs") == []


def test_status_reader_failure_retries_without_resubmitting(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: (_ for _ in ()).throw(TimeoutError()),
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "retry_wait"
    assert outcome.error_code == "vm_status_reader_unavailable"
    watch = collector.store.list_rows("rca_execution_watch")[0]
    assert watch["last_error_code"] == "vm_status_reader_unavailable"
    assert collector.store.list_rows("rca_delivery_effects") == []


def test_terminal_blocker_creates_durable_issue_failure_effect(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "blocked",
            "blocker": {"kind": "need_keyframe", "message": "no candidate frame"},
        },
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == "vm_terminal_blocked_need_keyframe"
    watch = collector.store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "delivery_created"
    assert watch["last_error_detail"] == "no candidate frame"
    job = collector.store.list_rows("rca_delivery_jobs")[0]
    assert job["outcome"] == "terminal_failed"
    assert job["terminal_state"] == "blocked"
    assert job["terminal_error_code"] == "vm_terminal_blocked_need_keyframe"
    assert json.loads(job["manifest_json"]) == {}
    assert json.loads(job["contract_json"]) == {}
    assert json.loads(job["artifacts_json"]) == []
    effect = collector.store.list_rows("rca_delivery_effects")[0]
    assert effect["effect_kind"] == "feishu_issue_comment"
    assert effect["outcome"] == "terminal_failed"
    assert effect["status"] == "pending"
    payload = json.loads(effect["payload_json"])
    assert payload["error_code"] == "vm_terminal_blocked_need_keyframe"
    assert "no candidate frame" not in json.dumps(payload, ensure_ascii=False)
    assert "report_url" not in payload


@pytest.mark.parametrize(
    ("blocker_kind", "expected_code"),
    [
        ("need_key_frame", "vm_terminal_blocked_need_keyframe"),
        ("", "vm_terminal_blocked_unclassified"),
        (
            "auth bearer sk_live_SUPERSECRET123",
            "vm_terminal_blocked_unclassified",
        ),
    ],
)
def test_terminal_error_codes_are_stably_sanitized(
    tmp_path, blocker_kind, expected_code
):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "blocked",
            "blocker": {"kind": blocker_kind, "message": "private detail"},
        },
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "terminal_failed"
    assert outcome.error_code == expected_code
    payload = json.loads(
        collector.store.list_rows("rca_delivery_effects")[0]["payload_json"]
    )
    assert payload["error_code"] == expected_code
    assert "private detail" not in json.dumps(payload, ensure_ascii=False)


def test_unknown_internal_quarantine_code_is_not_exposed(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        bundle_reader=lambda _claim: (_ for _ in ()).throw(
            ArtifactBundleReadError("UPSTREAM:BAD CODE", permanent=True)
        ),
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "quarantined"
    assert outcome.error_code == "terminal_failure_unclassified"
    payload = json.loads(
        collector.store.list_rows("rca_delivery_effects")[0]["payload_json"]
    )
    assert payload["error_code"] == "terminal_failure_unclassified"
    assert "upstream_bad_code" not in json.dumps(payload, ensure_ascii=False)


def test_manual_terminal_failure_materializes_issue_and_origin_topic(tmp_path):
    control, _result = _control(tmp_path)
    trigger = control.list_rows("business_triggers")[0]
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    identity = SimpleNamespace(
        business_key=trigger["business_key"],
        generation=trigger["generation"],
        project_key=trigger["project_key"],
        work_item_type_key=trigger["work_item_type_key"],
        work_item_id=trigger["work_item_id"],
    )
    _insert_subscription(store, identity, effect_kind="feishu_thread_reply")
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "failed",
            "error": "raw stack trace must stay internal",
        },
    )

    outcome = collector.collect_batch()[0]

    assert outcome.status == "terminal_failed"
    effects = collector.store.list_rows("rca_delivery_effects")
    assert {row["effect_kind"] for row in effects} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }
    assert {row["outcome"] for row in effects} == {"terminal_failed"}
    assert all(
        "raw stack trace" not in row["payload_json"] for row in effects
    )
    thread_payload = json.loads(
        next(
            row["payload_json"]
            for row in effects
            if row["effect_kind"] == "feishu_thread_reply"
        )
    )
    assert thread_payload["chat_id"] == "oc_group123"
    assert thread_payload["thread_id"] == "topic:om_root123"


def test_late_manual_topic_subscription_catches_up_terminal_failure(tmp_path):
    control, _result = _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "failed",
        },
    )
    assert collector.collect_batch()[0].status == "terminal_failed"
    job = collector.store.list_rows("rca_delivery_jobs")[0]
    trigger = control.list_rows("business_triggers")[0]
    _insert_subscription(
        collector.store,
        SimpleNamespace(
            business_key=trigger["business_key"],
            generation=trigger["generation"],
            project_key=trigger["project_key"],
            work_item_type_key=trigger["work_item_type_key"],
            work_item_id=trigger["work_item_id"],
        ),
        effect_kind="feishu_thread_reply",
    )

    result = collector.store.materialize_pending_subscriptions(now=NOW)

    assert result.materialized == 1
    effects = collector.store.list_rows("rca_delivery_effects")
    assert len(effects) == 2
    assert {row["delivery_id"] for row in effects} == {job["delivery_id"]}
    assert {row["outcome"] for row in effects} == {"terminal_failed"}


def test_each_late_manual_topic_gets_its_own_terminal_effect(tmp_path):
    control, _result = _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "failed",
        },
    )
    assert collector.collect_batch()[0].status == "terminal_failed"
    trigger = control.list_rows("business_triggers")[0]
    identity = SimpleNamespace(
        business_key=trigger["business_key"],
        generation=trigger["generation"],
        project_key=trigger["project_key"],
        work_item_type_key=trigger["work_item_type_key"],
        work_item_id=trigger["work_item_id"],
    )
    _insert_subscription(
        collector.store,
        identity,
        effect_kind="feishu_thread_reply",
        thread_root="om_late_root_one",
        source_message_id="om_late_trigger_one",
    )
    _insert_subscription(
        collector.store,
        identity,
        effect_kind="feishu_thread_reply",
        thread_root="om_late_root_two",
        source_message_id="om_late_trigger_two",
    )

    result = collector.store.materialize_pending_subscriptions(now=NOW)

    assert result.materialized == 2
    effects = collector.store.list_rows("rca_delivery_effects")
    assert len(effects) == 3
    thread_effects = [
        row for row in effects if row["effect_kind"] == "feishu_thread_reply"
    ]
    assert {row["target_key"] for row in thread_effects} == {
        "feishu_thread:oc_group123:om_late_root_one",
        "feishu_thread:oc_group123:om_late_root_two",
    }
    assert len({row["effect_key"] for row in thread_effects}) == 2


def test_required_terminal_failure_pending_prevents_new_generation(tmp_path):
    control, _result = _control(tmp_path)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "failed",
        },
    )
    assert collector.collect_batch()[0].status == "terminal_failed"

    joined = control.admit_manual_trigger(
        ManualRcaTriggerRequest(
            schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
            issue_url=(
                "https://project.feishu.cn/g1q3/issue/detail/7041712812"
            ),
            mode="rerun",
            reason="manual_explicit_issue_action",
            platform="feishu",
            chat_id="oc_group123",
            thread_id="topic:om_pending_failure_root",
            message_id="om_pending_failure_trigger",
            requester_id="ou_requester789",
        ),
        allowed_chat_ids={"oc_group123"},
        submit_enabled=True,
        operator_authorized=True,
    )

    assert joined.generation == 1
    assert joined.outcome == "catchup_attached"
    assert len(control.list_rows("business_triggers")) == 1


def test_terminal_pipeline_failures_do_not_block_their_delivery_circuit(tmp_path):
    _control(tmp_path, offset=10, issue_id=7041712812)
    _control(tmp_path, offset=11, issue_id=7041712813)
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "blocked",
            "blocker": {"kind": "required_input", "message": "input unavailable"},
        },
    )

    outcomes = collector.collect_batch()

    assert [outcome.status for outcome in outcomes[:2]] == [
        "terminal_failed",
        "terminal_failed",
    ]
    assert collector.store.delivery_dispatcher_circuit().is_open is False
    assert {row["outcome"] for row in collector.store.list_rows("rca_delivery_jobs")} == {
        "terminal_failed"
    }
    assert len(collector.store.list_rows("rca_delivery_effects")) == 2


def test_completed_task_without_manifest_retries_then_quarantines_after_grace(tmp_path):
    _control(tmp_path)
    clock = {"now": NOW}
    collector = _collector(
        tmp_path,
        now=lambda: clock["now"],
        bundle_reader=lambda claim: {
            "delivery_contract": _bundle_payload()["delivery_contract"],
            "delivery_manifest": {},
            "observed_files": [],
        },
    )

    first = collector.collect_batch()[0]
    assert first.status == "retry_wait"
    assert first.error_code == "terminal_artifact_pending"
    watch = collector.store.list_rows("rca_execution_watch")[0]
    assert watch["terminal_first_seen_at"] == NOW.isoformat()

    clock["now"] = NOW + timedelta(seconds=901)
    outcome = collector.collect_batch()[0]
    assert outcome.status == "quarantined"
    assert outcome.error_code == "terminal_artifact_grace_exceeded"
    watch = collector.store.list_rows("rca_execution_watch")[0]
    assert watch["state"] == "delivery_created"
    job = collector.store.list_rows("rca_delivery_jobs")[0]
    assert job["outcome"] == "quarantined"
    assert collector.store.list_rows("rca_delivery_effects")[0]["outcome"] == (
        "quarantined"
    )


def test_default_reader_missing_manifest_error_uses_same_grace(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        bundle_reader=lambda claim: (_ for _ in ()).throw(
            ArtifactBundleReadError("delivery_manifest_missing", permanent=True)
        ),
    )

    outcome = collector.collect_batch()[0]
    assert outcome.status == "retry_wait"
    assert outcome.error_code == "terminal_artifact_pending"


def test_default_reader_treats_remote_parser_outage_as_retryable(monkeypatch):
    payload = {
        "ok": False,
        "error_code": "html_css_parser_dependency_missing",
        "error": "html_css_parser_dependency_missing",
    }
    monkeypatch.setattr(
        collector_module.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, stdout=json.dumps(payload), stderr=""
        ),
    )
    claim = type(
        "Claim",
        (),
        {"submission_key": "g1q3-rca-s1-" + "a" * 64},
    )()

    with pytest.raises(ArtifactBundleReadError) as error:
        collector_module.default_artifact_bundle_reader(claim)

    assert error.value.code == "html_css_parser_dependency_missing"
    assert error.value.permanent is False


def test_artifact_bundle_can_appear_during_terminal_grace(tmp_path):
    _control(tmp_path)
    reads = {"count": 0}
    clock = {"now": NOW}

    def reader(_claim):
        reads["count"] += 1
        if reads["count"] == 1:
            raise ArtifactBundleReadError("delivery_manifest_missing")
        return _bundle_payload()

    collector = _collector(tmp_path, bundle_reader=reader, now=lambda: clock["now"])
    assert collector.collect_batch()[0].status == "retry_wait"
    clock["now"] = NOW + timedelta(seconds=21)
    assert collector.collect_batch()[0].status == "delivery_created"


def test_reader_transport_error_retries_instead_of_quarantining(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        bundle_reader=lambda claim: (_ for _ in ()).throw(
            ArtifactBundleReadError("artifact_reader_unavailable")
        ),
    )

    outcome = collector.collect_batch()[0]
    assert outcome.status == "retry_wait"
    assert outcome.error_code == "artifact_reader_unavailable"


def test_hash_mismatch_is_permanent_and_enqueues_sanitized_failure(tmp_path):
    _control(tmp_path)
    bundle = _bundle_payload()
    bundle["observed_files"][0]["sha256"] = "0" * 64
    collector = _collector(tmp_path, bundle_reader=lambda claim: bundle)

    outcome = collector.collect_batch()[0]

    assert outcome.status == "quarantined"
    assert outcome.error_code == "artifact_hash_mismatch"
    job = collector.store.list_rows("rca_delivery_jobs")[0]
    assert job["outcome"] == "quarantined"
    effect = collector.store.list_rows("rca_delivery_effects")[0]
    payload = json.loads(effect["payload_json"])
    assert payload["error_code"] == "artifact_hash_mismatch"
    assert json.loads(job["artifacts_json"]) == []


def test_dry_run_reads_status_but_creates_no_watch_or_effect(tmp_path):
    _control(tmp_path)
    seen = []
    collector = _collector(
        tmp_path,
        status_reader=lambda task_id: (
            seen.append(task_id)
            or {"success": True, "task_id": task_id, "state": "running"}
        ),
    )

    preview = collector.dry_run_once()

    assert preview["dry_run"] is True
    assert preview["external_writes"] is False
    assert preview["candidate_count"] == 1
    assert seen[0].startswith("g1q3-rca-s1-")
    assert collector.store.list_rows("rca_execution_watch") == []
    assert collector.store.list_rows("rca_delivery_effects") == []


def test_disabled_loop_never_reads_status_or_artifacts(tmp_path):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        enabled=False,
        status_reader=lambda task_id: (_ for _ in ()).throw(
            AssertionError("disabled collector must not read status")
        ),
        bundle_reader=lambda claim: (_ for _ in ()).throw(
            AssertionError("disabled collector must not read artifacts")
        ),
    )

    assert run_collector_loop(collector, once=True) == 0
    assert collector.store.list_rows("rca_execution_watch") == []
    healthy, payload = read_health(collector.config.health_path, max_age_seconds=60)
    assert healthy is True
    assert payload["state"] == "disabled"
    assert payload["schema_version"] == "pnc_rca_delivery_collector_health_v2"
    assert payload["runtime_identity"]["service_label"] == (
        "local.pnc.rca-delivery-collector"
    )
    assert payload["dependencies"]["remote_css_parser"]["status"] == "disabled"


def test_enabled_health_binds_runtime_identity_and_fresh_remote_parser_receipt(
    tmp_path,
):
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=True)
    calls = []

    def probe(agent, *, timeout_seconds):
        calls.append((agent, timeout_seconds))
        return collector_module.expected_remote_css_runtime_dependency()

    reporter = HealthReporter(
        collector.config,
        collector.store,
        remote_css_probe=probe,
    )
    reporter.write(state="idle", stats=collector.stats)
    payload = json.loads(collector.config.health_path.read_text(encoding="utf-8"))

    assert calls == [(collector.config.ssh_mini_agent, 15)]
    assert payload["schema_version"] == "pnc_rca_delivery_collector_health_v2"
    assert payload["runtime_identity"]["service_label"] == (
        "local.pnc.rca-delivery-collector"
    )
    assert len(payload["runtime_identity"]["script_sha256"]) == 64
    assert len(payload["runtime_identity"]["runtime_files_sha256"]) == 64
    assert len(payload["runtime_identity"]["public_config_sha256"]) == 64
    assert len(payload["runtime_identity"]["loaded_runtime_sha256"]) == 64
    receipt = payload["dependencies"]["remote_css_parser"]
    assert receipt == {
        **collector_module.expected_remote_css_runtime_dependency(),
        "observed_at": receipt["observed_at"],
    }


def test_health_rejects_identity_without_loaded_runtime_digest(tmp_path):
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=False)
    reporter = HealthReporter(collector.config, collector.store)
    reporter.write(state="disabled", stats=collector.stats)
    path = collector.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["runtime_identity"].pop("loaded_runtime_sha256")
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60)

    assert healthy is False
    assert result["error"] == "health_runtime_identity_invalid"


def test_transient_remote_parser_probe_failure_is_unhealthy_without_double_probe(
    tmp_path,
):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        enabled=True,
        status_reader=lambda _task_id: (_ for _ in ()).throw(
            AssertionError("dependency outage must block status reads")
        ),
        bundle_reader=lambda _claim: (_ for _ in ()).throw(
            AssertionError("dependency outage must block artifact reads")
        ),
    )
    calls = []

    def unavailable(_agent, *, timeout_seconds):
        calls.append(timeout_seconds)
        raise collector_module.ArtifactBundleReadError(
            "html_css_parser_probe_unavailable",
            "simulated outage",
            permanent=True,
        )

    assert (
        run_collector_loop(
            collector,
            once=True,
            remote_css_probe=unavailable,
        )
        == 2
    )
    payload = json.loads(collector.config.health_path.read_text(encoding="utf-8"))
    assert calls == [15]
    assert payload["healthy"] is False
    assert payload["state"] == "error"
    assert payload["dependency_error"] == "html_css_parser_probe_unavailable"
    assert payload["dependencies"]["remote_css_parser"]["status"] == ("unavailable")


def test_health_fails_when_heartbeat_is_stale(tmp_path):
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=False)
    reporter = HealthReporter(collector.config, collector.store)
    reporter.write(state="disabled", stats=collector.stats)
    path = collector.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = "2000-01-01T00:00:00+00:00"
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60)
    assert healthy is False
    assert result["age_seconds"] > 60


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
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=False)
    reporter = HealthReporter(collector.config, collector.store)
    reporter.write(state="disabled", stats=collector.stats)
    path = collector.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = (NOW + timedelta(seconds=future_seconds)).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60, now=NOW)

    assert healthy is expected_healthy
    assert result["age_seconds"] == -future_seconds
    assert (result.get("error") or None) == expected_error


def test_health_rejects_timezone_naive_heartbeat(tmp_path):
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=False)
    reporter = HealthReporter(collector.config, collector.store)
    reporter.write(state="disabled", stats=collector.stats)
    path = collector.config.health_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = "2026-07-10T00:00:00"
    path.write_text(json.dumps(payload), encoding="utf-8")

    healthy, result = read_health(path, max_age_seconds=60, now=NOW)

    assert healthy is False
    assert result["error"] == "health_timestamp_invalid"


def test_remote_bundle_script_is_root_bound_and_read_only():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    script = collector_module._remote_bundle_script(submission_key)

    compile(script, "<remote-bundle-reader>", "exec")
    assert f"/mnt/tmp/{submission_key}/" in script
    assert "os.lstat" in script
    assert "O_NOFOLLOW" in script
    assert "hashlib.sha256" in script
    assert "MAX_TOTAL_BYTES = 512 * 1024 * 1024" in script
    assert "html_delivery_mcap_forbidden" in script
    for mutating_token in ("unlink(", "remove(", "rmtree(", "write(", "subprocess"):
        assert mutating_token not in script


def test_remote_bundle_script_proves_recursive_html_css_dependency_closure(
    tmp_path,
):
    files = {
        "index.html": """
            <link rel="stylesheet" href="assets/app.css">
            <img srcset="img/one.png 1x, img/two.png 2x">
        """,
        "assets/app.css": (
            '@import "theme/base.css";body{background:url("../img/background.png")}'
        ),
        "assets/theme/base.css": '@font-face{src:url("../../fonts/a.woff2")}',
        "img/one.png": b"one",
        "img/two.png": b"two",
        "img/background.png": b"background",
        "fonts/a.woff2": b"font",
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is True
    relative_dependencies = {
        str(Path(path).relative_to(tmp_path / "bundle"))
        for path in payload["html_dependencies"]
    }
    assert relative_dependencies == set(files) - {"index.html"}


@pytest.mark.parametrize(
    "css_text",
    [
        "body{width:10px}",
        "body{opacity:.5}",
        "body{font-weight:700}",
        "body{color:#fff}",
    ],
)
def test_remote_bundle_script_accepts_scalar_css_tokens(tmp_path, css_text):
    files = {
        "index.html": '<link rel="stylesheet" href="assets/app.css">',
        "assets/app.css": css_text,
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is True
    assert len(payload["html_dependencies"]) == 1


def test_remote_bundle_script_rejects_unmanifested_nested_dependency(tmp_path):
    files = {
        "index.html": '<link rel="stylesheet" href="assets/app.css">',
        "assets/app.css": '@import "missing.css";',
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is False
    assert payload["error_code"] == "html_dependency_not_manifested"


@pytest.mark.parametrize(
    ("files", "expected_dependency"),
    [
        (
            {
                "index.html": '<link rel="stylesheet" href="style.txt">',
                "style.txt": "body{background:url(hidden.png)}",
            },
            "hidden.png",
        ),
        (
            {
                "index.html": '<link rel="stylesheet" href="style.css">',
                "style.css": '@import "theme.bin";',
                "theme.bin": "body{background:url(hidden.png)}",
            },
            "hidden.png",
        ),
    ],
)
def test_remote_bundle_script_preserves_browser_css_loading_context(
    tmp_path, files, expected_dependency
):
    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert expected_dependency not in files
    assert payload["ok"] is False
    assert payload["error_code"] == "html_dependency_not_manifested"


def test_remote_bundle_script_rejects_computed_dynamic_dependency(tmp_path):
    files = {
        "index.html": '<script src="assets/app.js"></script>',
        "assets/app.js": 'fetch(assetRoot + "/report.json");',
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is False
    assert payload["error_code"] == "html_script_execution_unsupported"


def test_remote_bundle_script_rejects_external_nested_dependency(tmp_path):
    files = {
        "index.html": '<link rel="stylesheet" href="assets/app.css">',
        "assets/app.css": 'body{background:url("https://example.com/a.png")}',
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is False
    assert payload["error_code"] == "html_external_dependency_unsupported"


def test_remote_bundle_script_closes_structured_html_loading_bypasses(tmp_path):
    html_cases = (
        ("<img src=missing.png>", "html_dependency_not_manifested"),
        (
            '<svg><image href="missing.png"></image></svg>',
            "html_dependency_not_manifested",
        ),
        (
            '<iframe srcdoc="&lt;img src=&quot;missing.png&quot;&gt;"></iframe>',
            "html_active_content_unsupported",
        ),
        ('<!--><img src="hidden.png">-->', "html_comments_unsupported"),
    )
    for index, (html_text, expected_error) in enumerate(html_cases):
        case_root = tmp_path / str(index)
        files = {"index.html": html_text}

        payload = _run_remote_bundle_script(case_root, files, _artifact_rows(files))

        assert payload["ok"] is False
        assert payload["error_code"] == expected_error


@pytest.mark.parametrize(
    ("html_text", "expected_error"),
    [
        (
            '<style>body{background-image:image-set("hidden.png" 1x)}</style>',
            "html_css_dynamic_resource_unsupported",
        ),
        (
            r'<style>body{background-image:u\72l("hidden.png")}</style>',
            "html_dependency_not_manifested",
        ),
        (
            '<link rel="preload" as="image" imagesrcset="hidden.png 1x">',
            "html_dependency_not_manifested",
        ),
        ('<body background="hidden.png">', "html_dependency_not_manifested"),
        (
            '<table background="hidden.png"><tr><td>x</td></tr></table>',
            "html_dependency_not_manifested",
        ),
        (
            '<svg><rect fill="url(hidden.svg#p)"></rect></svg>',
            "html_dependency_not_manifested",
        ),
    ],
)
def test_remote_bundle_script_closes_browser_observed_static_resource_bypasses(
    tmp_path, html_text, expected_error
):
    files = {"index.html": html_text}

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is False
    assert payload["error_code"] == expected_error


def test_remote_bundle_script_rejects_recursive_external_svg_documents(tmp_path):
    files = {
        "index.html": '<svg><use href="icon.svg#shape"></use></svg>',
        "icon.svg": (
            '<svg xmlns="http://www.w3.org/2000/svg">'
            '<g id="shape"><image href="hidden.png"/></g></svg>'
        ),
    }

    payload = _run_remote_bundle_script(tmp_path, files, _artifact_rows(files))

    assert payload["ok"] is False
    assert payload["error_code"] == "html_external_active_document_unsupported"


def test_remote_bundle_script_rejects_active_embedded_data_documents(tmp_path):
    html_cases = (
        (
            '<svg><image href="data:text/html,%3Cscript%3Efetch('
            "'https://example.com/x')%3C/script%3E\"></image></svg>"
        ),
        '<img src="data:image/svg+xml,%3Csvg%3E%3C/svg%3E">',
    )
    for index, html_text in enumerate(html_cases):
        case_root = tmp_path / str(index)
        files = {"index.html": html_text}

        payload = _run_remote_bundle_script(case_root, files, _artifact_rows(files))

        assert payload["ok"] is False
        assert payload["error_code"] == ("html_embedded_data_dependency_unsupported")


def test_remote_bundle_script_rejects_executable_html_content(tmp_path):
    html_cases = (
        '<script src="assets/app.js"></script>',
        "<button onclick=\"fetch('hidden.json')\">open</button>",
        "<a href=\"javascript:fetch('hidden.json')\">open</a>",
        "<a href=\"java&#10;script:document.body.dataset.pwned='yes'\">open</a>",
        "<a href=\"jav&#9;ascript:document.body.dataset.pwned='yes'\">open</a>",
        "<a href=\"java&#13;script:document.body.dataset.pwned='yes'\">open</a>",
    )
    for index, html_text in enumerate(html_cases):
        case_root = tmp_path / str(index)
        files = {"index.html": html_text, "assets/app.js": b"void 0;"}

        payload = _run_remote_bundle_script(case_root, files, _artifact_rows(files))

        assert payload["ok"] is False
        assert payload["error_code"] == "html_script_execution_unsupported"


def test_artifact_reader_timeout_is_capped_at_110_seconds(tmp_path):
    try:
        CollectorConfig.from_env(
            {"HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "111"},
            hermes_home=tmp_path,
        )
    except ValueError as exc:
        assert "at most 110" in str(exc)
    else:
        raise AssertionError("timeout above 110 seconds must be rejected")


def test_collector_writes_periodic_health_during_long_batch(tmp_path, monkeypatch):
    _control(tmp_path)
    collector = _collector(tmp_path, enabled=True)
    writes = []
    original_write = HealthReporter.write

    def observed_write(self, **kwargs):
        writes.append(kwargs["state"])
        return original_write(self, **kwargs)

    def slow_batch():
        time.sleep(0.05)
        return [collector_module.CollectOutcome(status="idle")]

    monkeypatch.setattr(HealthReporter, "write", observed_write)
    monkeypatch.setattr(
        collector_module,
        "_heartbeat_interval_seconds",
        lambda _max_age: 0.01,
    )
    monkeypatch.setattr(collector, "collect_batch", slow_batch)

    assert (
        run_collector_loop(
            collector,
            once=True,
            remote_css_probe=lambda *_args, **_kwargs: (
                collector_module.expected_remote_css_runtime_dependency()
            ),
        )
        == 0
    )
    assert writes.count("running") >= 2


def test_candidate_launchd_runs_collector_without_inline_secrets():
    root = Path(__file__).resolve().parents[2]
    path = root / "local.pnc.rca-delivery-collector.candidate.plist"
    payload = plistlib.loads(path.read_bytes())

    assert payload["Label"] == "local.pnc.rca-delivery-collector"
    assert payload["ProgramArguments"][-1].endswith(
        "/scripts/pnc_rca_delivery_collector.py"
    )
    assert payload["EnvironmentVariables"]["PYTHONNOUSERSITE"] == "1"
    serialized = path.read_text(encoding="utf-8").lower()
    for forbidden in ("password", "secret", "token", "app_secret"):
        assert forbidden not in serialized


class _CapacityControl:
    def __init__(self, state):
        self.state = state

    def capacity_transition_state(self):
        return dict(self.state)


class _CapacityCandidateStore:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = []

    def capacity_sample_candidates(
        self, *, activated_at, limit, excluded_task_attempts
    ):
        self.calls.append({
            "activated_at": activated_at,
            "limit": limit,
            "excluded": set(excluded_task_attempts),
        })
        identity = (
            self.snapshot.payload["task_id"],
            self.snapshot.payload["attempt_id"],
        )
        return [] if identity in excluded_task_attempts else [self.snapshot]


def _capacity_collector_fixture(tmp_path, monkeypatch):
    raw, meta, activation, _admission = capacity_terminal()
    payload = capacity_snapshot(meta)
    snapshot = DeliveryCapacitySnapshot(
        payload=payload,
        snapshot_sha256=hashlib.sha256(canonical_bytes(payload)).hexdigest(),
    )
    state = bootstrap_state()
    state["release_id"] = CAPACITY_RELEASE_ID
    store = _CapacityCandidateStore(snapshot)
    config = _config(tmp_path, capacity_sample_enabled=True)
    paths = CapacityRuntimePaths.from_control_db(config.control_db_path)
    write_owner_only_create_once(
        producer_activation_path(paths.state_root), activation
    )
    monkeypatch.setenv(
        "HERMES_RCA_PROD_ADMISSION_HMAC_KEY", "hex:" + CAPACITY_KEY.hex()
    )
    collector = DeliveryCollector(
        store=store,
        config=config,
        terminal_receipt_reader=lambda task_id, attempt_id: raw,
        capacity_control_store=_CapacityControl(state),
        now=lambda: NOW + timedelta(seconds=50),
    )
    return collector, store, paths, state


def test_capacity_sample_collector_appends_once_and_excludes_ledger_seen(
    tmp_path, monkeypatch
):
    collector, store, paths, _state = _capacity_collector_fixture(
        tmp_path, monkeypatch
    )

    collector.collect_capacity_samples()
    collector.collect_capacity_samples()

    assert collector.stats.capacity_scanned == 1
    assert collector.stats.capacity_eligible == 1
    assert collector.stats.capacity_appended == 1
    assert collector.stats.capacity_rejected == 0
    assert collector.stats.capacity_last_error == ""
    assert store.calls[1]["excluded"] == {
        (store.snapshot.payload["task_id"], store.snapshot.payload["attempt_id"])
    }
    ledger = collector_module.capacity_transition.read_sample_ledger(
        paths.sample_ledger, hmac_key=CAPACITY_KEY
    )
    assert ledger.sample_count == 1


def test_capacity_remote_read_never_holds_global_exclusive_lock(
    tmp_path, monkeypatch
):
    collector, _store, _paths, _state = _capacity_collector_fixture(
        tmp_path, monkeypatch
    )
    original_lock = collector_module.capacity_transition.capacity_flock
    exclusive_depth = 0
    terminal_reader = collector.terminal_receipt_reader

    @contextmanager
    def observed_lock(path, *, exclusive, timeout_seconds=5.0):
        nonlocal exclusive_depth
        with original_lock(
            path, exclusive=exclusive, timeout_seconds=timeout_seconds
        ) as descriptor:
            if exclusive:
                exclusive_depth += 1
            try:
                yield descriptor
            finally:
                if exclusive:
                    exclusive_depth -= 1

    def observed_reader(task_id, attempt_id):
        assert exclusive_depth == 0
        return terminal_reader(task_id, attempt_id)

    monkeypatch.setattr(
        collector_module.capacity_transition, "capacity_flock", observed_lock
    )
    collector.terminal_receipt_reader = observed_reader

    collector.collect_capacity_samples()

    assert collector.stats.capacity_appended == 1


def test_capacity_sample_collectors_concurrently_append_only_once(
    tmp_path, monkeypatch
):
    first, store, paths, state = _capacity_collector_fixture(tmp_path, monkeypatch)
    second = DeliveryCollector(
        store=store,
        config=first.config,
        terminal_receipt_reader=first.terminal_receipt_reader,
        capacity_control_store=_CapacityControl(state),
        now=first.now,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda collector: collector.collect_capacity_samples(), (first, second)))

    assert first.stats.capacity_appended + second.stats.capacity_appended == 1
    assert collector_module.capacity_transition.read_sample_ledger(
        paths.sample_ledger, hmac_key=CAPACITY_KEY
    ).sample_count == 1


def test_capacity_sample_steady_latch_is_healthy_frozen_noop(tmp_path, monkeypatch):
    collector, store, paths, state = _capacity_collector_fixture(
        tmp_path, monkeypatch
    )
    state["state"] = collector_module.capacity_transition.STEADY_ACTIVE
    collector.capacity_control_store = _CapacityControl(state)

    collector.collect_capacity_samples()

    assert collector.stats.capacity_frozen == 1
    assert collector.stats.capacity_appended == 0
    assert collector.stats.capacity_last_error == ""
    assert collector.capacity_last_outcome == "frozen"
    assert not paths.sample_ledger.exists()
    assert store.calls == []


def test_capacity_sample_rejection_surfaces_in_heartbeat_health(
    tmp_path, monkeypatch
):
    collector, _store, _paths, _state = _capacity_collector_fixture(
        tmp_path, monkeypatch
    )
    collector.terminal_receipt_reader = lambda *_args: b"{}"
    collector.collect_capacity_samples()
    reporter = HealthReporter(
        collector.config,
        RcaDeliveryStore(collector.config.control_db_path),
        remote_css_probe=lambda *_args, **_kwargs: (
            collector_module.expected_remote_css_runtime_dependency()
        ),
    )
    reporter.write(state="running", stats=collector.stats)
    payload = json.loads(collector.config.health_path.read_text())

    assert payload["healthy"] is False
    assert payload["capacity_samples"]["rejected"] == 1
    assert payload["capacity_samples"]["last_error"]
