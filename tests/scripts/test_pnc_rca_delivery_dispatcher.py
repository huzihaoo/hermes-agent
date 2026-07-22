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

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_V1,
    DeliveryContractError,
    build_report_url,
    build_terminal_delivery,
)
from gateway.pnc_rca_delivery_store import (
    RcaDeliveryStore,
    StaleDeliveryEffectLeaseError,
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
    _bind_activation_execution,
    _control,
    _insert_subscription,
    _switch_activation_epoch,
)
from tests.gateway.test_pnc_rca_delivery_contract import _bundle


@pytest.fixture(autouse=True)
def _seal_dependencies_available_in_the_test_interpreter(monkeypatch):
    monkeypatch.setenv("PNC_FOXGLOVE_RENDER_HOST", "https://viewer.internal")
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
        },
        hermes_home=tmp_path,
    )


def _collector(
    tmp_path,
    *,
    status_reader=None,
    bundle_reader=None,
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
        now=now or (lambda: NOW),
        lease_owner="collector-test",
    )


def _seed(tmp_path, *, bundle_payload=None):
    _control(tmp_path)
    collector = _collector(
        tmp_path,
        bundle_reader=(
            None if bundle_payload is None else lambda _claim: bundle_payload
        ),
    )
    assert collector.collect_batch()[0].status == "delivery_created"
    return collector.store


def _seed_with_thread_subscription(tmp_path, *, bundle_payload=None):
    control, _result = _control(tmp_path)
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
    control, _result = _control(tmp_path)
    if with_thread:
        trigger = control.list_rows("business_triggers")[0]
        _insert_subscription(
            RcaDeliveryStore(tmp_path / "control.sqlite3"),
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
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "failed",
            "error": "sensitive backend detail",
        },
    )
    assert collector.collect_batch()[0].status == "terminal_failed"
    return collector.store


def _web_bundle_payload():
    _admission, contract, manifest, observed, dependencies = _bundle(
        include_web_assets=True
    )
    return {
        "delivery_contract": contract,
        "delivery_manifest": manifest,
        "observed_files": observed,
        "html_dependencies": dependencies,
    }


def _asset_relative(url):
    route = url.split("/G1Q3_RCA/cases/", 1)[1]
    _submission_key, _artifact_set_id, relative = route.split("/", 2)
    return relative


class Remote:
    def __init__(self):
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
        assert project_key == "t03o4q"
        assert work_item_id == "7041712812"
        self.list_calls += 1
        self.history.append("list_comments")
        if self.list_failure is not None:
            return dict(self.list_failure)
        return {"success": True, "comments": list(self.comments)}

    def add_comment(self, project_key, work_item_id, content):
        assert project_key == "t03o4q"
        assert work_item_id == "7041712812"
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
        assert project_key == "t03o4q"
        assert work_item_id == "7041712812"
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
        assert project_key == "t03o4q"
        assert work_item_id == "7041712812"
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
    assert url.startswith("https://viewer.internal/G1Q3_RCA/cases/")
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


def test_default_config_is_disabled_and_comment_write_is_closed(tmp_path):
    config = DispatcherConfig.from_env({}, hermes_home=tmp_path)
    assert config.enabled is False
    assert config.public_dict()["external_writes"] is False
    assert config.public_dict()["allowed_effect_kind"] == "feishu_issue_comment"
    assert config.public_dict()["allowed_effect_kinds"] == [
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
                    "body": {"content": json.dumps({"text": "marker\nreport"})},
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
        "comments": [{"remote_id": "om_reply456", "content": "marker\nreport"}],
        "pages_read": 1,
    }


def test_feishu_thread_writer_preserves_topic_and_stable_uuid():
    calls = []

    async def send(chat_id, content, metadata=None):
        calls.append((chat_id, content, metadata))
        return SimpleNamespace(success=True, message_id="om_reply456", error=None)

    fake_adapter = SimpleNamespace(send=send)
    result = FeishuThreadReplyAdapter(fake_adapter).add_reply(
        "oc_group123",
        "topic:om_root123",
        "marker\nreport",
        "00000000-0000-0000-0000-000000000001",
    )

    assert result == {"success": True, "remote_id": "om_reply456"}
    assert calls == [
        (
            "oc_group123",
            "marker\nreport",
            {
                "thread_id": "topic:om_root123",
                "idempotency_uuid": "00000000-0000-0000-0000-000000000001",
            },
        )
    ]


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

    result = FeishuThreadReplyAdapter(
        SimpleNamespace(send=slow_send)
    ).add_reply(
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

    assert outcome.status == "succeeded"
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
    assert receipt["confirmed_content_sha256"] == hashlib.sha256(
        payload["comment_content"].encode("utf-8")
    ).hexdigest()


def test_postwrite_marker_without_canonical_body_never_acks(tmp_path):
    _seed(tmp_path)

    class TruncatingRemote(Remote):
        def add_comment(self, project_key, work_item_id, content):
            result = super().add_comment(project_key, work_item_id, content)
            self.comments[-1]["content"] = content.splitlines()[0]
            return result

    remote = TruncatingRemote()
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "delivery_remote_content_mismatch"
    assert remote.add_calls == 1
    assert remote.update_field_calls == 1
    assert remote.comments[0]["content"].startswith("[RCA_DELIVERY:")


def test_existing_marker_repairs_drifted_fields_without_duplicate_comment(tmp_path):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    remote.comments.append({"remote_id": "comment-existing", "content": payload["comment_content"]})
    remote.fields = {"field_9193cb": "stale", "field_8c912e": ""}
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert outcome.remote_id == "comment-existing"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 1
    assert remote.fields == {
        item["field_key"]: item["field_value"]
        for item in payload["field_updates"]
    }
    receipt = json.loads(
        store.list_rows("rca_delivery_effects")[0]["remote_receipt_json"]
    )
    assert receipt["source"] == "field_repair_after_marker"
    assert receipt["confirmed_report_url"] == payload["report_url"]
    assert receipt["confirmed_content_sha256"] == hashlib.sha256(
        payload["comment_content"].encode("utf-8")
    ).hexdigest()


def test_meegle_normalized_marker_reconciles_without_duplicate_comment(tmp_path):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    normalized_content = payload["comment_content"].replace(
        payload["marker"], payload["marker"][1:-1], 1
    )
    remote.comments.append(
        {"remote_id": "comment-existing", "content": normalized_content}
    )
    remote.fields = {
        item["field_key"]: item["field_value"]
        for item in payload["field_updates"]
    }
    dispatcher, _remote, _clock = _dispatcher(tmp_path, remote=remote)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "reconciled"
    assert outcome.remote_id == "comment-existing"
    assert remote.add_calls == 0
    assert remote.update_field_calls == 0


def test_marker_only_remote_comment_is_quarantined_without_html_network_dependency(
    tmp_path,
):
    store = _seed(tmp_path)
    payload = json.loads(store.list_rows("rca_delivery_effects")[0]["payload_json"])
    remote = Remote()
    remote.comments.append(
        {"remote_id": "comment-marker-only", "content": payload["marker"]}
    )
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
    remote.comments.append(
        {"remote_id": "comment-existing", "content": payload["comment_content"]}
    )
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
    ]

    matches = dispatcher_module._marker_matches(comments, marker)

    assert [item["remote_id"] for item in matches] == ["normalized"]


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
    dispatcher, remote, _clock = _dispatcher(
        tmp_path, thread_remote=thread_remote
    )

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
    assert before["business_ready"] is False
    assert before["business_blockers"]["unresolved_required_effects"] == 2
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
    existing_report = foxglove_url(
        canonical_viz_mcap_path("older-generation-success")
    )
    assert existing_report
    remote.fields["field_8c912e"] = existing_report

    outcomes = [dispatcher.dispatch_one(), dispatcher.dispatch_one()]

    assert {outcome.status for outcome in outcomes} == {"succeeded"}
    assert verifier_calls == []
    assert remote.add_calls == 1
    assert remote.update_field_calls == 1
    assert "非归因结论" in remote.fields["field_9193cb"]
    assert "第 1 代" in remote.fields["field_9193cb"]
    assert "可能保留自其他代次" in remote.fields["field_9193cb"]
    assert remote.fields["field_8c912e"] == existing_report
    assert thread_remote.add_calls == 1
    assert "本终态不改写" in remote.comments[0]["content"]
    assert "不代表第 1 代结论" in remote.comments[0]["content"]
    assert "本终态不改写" in thread_remote.comments[0]["content"]
    assert "sensitive backend detail" not in remote.comments[0]["content"]
    assert "sensitive backend detail" not in thread_remote.comments[0]["content"]
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


def test_pre_submit_quarantine_keeps_specific_safe_diagnostic_only(tmp_path):
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

    [job] = store.list_rows("rca_delivery_jobs")
    [effect] = store.list_rows("rca_delivery_effects")
    contract = json.loads(job["contract_json"])
    payload = json.loads(effect["payload_json"])
    assert contract["diagnostic_code"] == "input_frame_required"
    assert payload["diagnostic_code"] == "input_frame_required"
    assert payload["error_code"] == "outbox_submission_quarantined"
    assert "issue_field_invalid_frame_reference" not in effect["payload_json"]
    assert "SECRET-MUST-NOT-LEAK" not in effect["payload_json"]


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
                json.dumps(
                    {
                        "field_updates": [
                            {
                                "field_key": "field_9193cb",
                                "field_value": "newer terminal result",
                            }
                        ]
                    }
                ),
                "6" * 64,
                current,
                current,
                current,
            ),
        )
    dispatcher, remote, _clock = _dispatcher(tmp_path)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "superseded"
    assert outcome.error_code == (
        "delivery_effect_superseded_by_newer_settled_fields"
    )
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
    assert store.suppress_terminal_effect_if_newer_settled_fields(
        claim=claim,
        now=NOW,
    ) is None
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
                json.dumps(
                    {
                        "field_updates": [
                            {
                                "field_key": "field_9193cb",
                                "field_value": "newer terminal result",
                            }
                        ]
                    }
                ),
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

    assert outcome.status == "lease_lost"
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
    dispatcher, remote, _clock = _dispatcher(
        tmp_path, thread_remote=thread_remote
    )

    first = dispatcher.dispatch_one()
    second = dispatcher.dispatch_one()

    assert first.status == "circuit_open"
    assert first.error_code == "feishu_auth_failed"
    assert second.status == "succeeded"
    assert remote.add_calls == 1
    assert store.delivery_dispatcher_circuit(
        "feishu_thread_reply"
    ).is_open is True
    assert store.delivery_dispatcher_circuit(
        "feishu_issue_comment"
    ).is_open is False


def test_delivery_does_not_fetch_supporting_html_before_foxglove_comment(tmp_path):
    _seed(tmp_path, bundle_payload=_web_bundle_payload())
    calls = []
    def verifier(url, size, sha256):
        calls.append((url, size, sha256))
        return _verified_report(url, size, sha256)

    remote = Remote()
    add_comment = remote.add_comment

    def guarded_add(project_key, work_item_id, content):
        assert calls == []
        return add_comment(project_key, work_item_id, content)

    remote.add_comment = guarded_add
    dispatcher, remote, _clock = _dispatcher(tmp_path, remote=remote, verifier=verifier)

    assert dispatcher.dispatch_one().status == "succeeded"
    assert calls == []
    assert remote.add_calls == 1


def test_dispatcher_rejects_report_url_for_another_submission_before_http(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    bad_url = build_report_url(
        "g1q3-rca-s1-" + "f" * 64, claim.artifact_set_id
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


def test_dispatcher_rejects_non_foxglove_report_link_kind_before_write(tmp_path):
    store = _seed(tmp_path)
    claim = store.claim_due_effect(lease_owner="worker-1", now=NOW)
    assert claim is not None
    tampered = replace(
        claim,
        payload={**claim.payload, "report_link_kind": "manifest_html"},
    )

    with pytest.raises(DeliveryContractError) as exc:
        dispatcher_module._validate_effect(tampered)

    assert exc.value.code == "delivery_effect_report_link_kind_invalid"


def test_dispatcher_rejects_manifest_html_report_field_before_write(tmp_path):
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


def test_foxglove_delivery_skips_html_artifact_network_loop(
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
    assert contender_claims == []
    assert clock.current == NOW
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
        if (
            threading.current_thread().name.startswith(
                f"{dispatcher_module.SERVICE_LABEL}-effect-lease-"
            )
            and kwargs["now"] >= NOW + timedelta(seconds=80)
        ):
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
        if (
            write_entered.is_set()
            and threading.current_thread().name.startswith(
                f"{dispatcher_module.SERVICE_LABEL}-effect-lease-"
            )
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
def test_changed_remote_html_assets_do_not_block_formal_foxglove_delivery(
    tmp_path, changed_asset
):
    _seed(tmp_path, bundle_payload=_web_bundle_payload())
    calls = []

    def verifier(url, size, sha256):
        relative = _asset_relative(url)
        calls.append(relative)
        if relative == changed_asset:
            return {
                "success": False,
                "permanent": True,
                "error_code": "report_http_hash_mismatch",
            }
        return _verified_report(url, size, sha256)

    dispatcher, remote, _clock = _dispatcher(tmp_path, verifier=verifier)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "succeeded"
    assert calls == []
    assert remote.add_calls == 1


def test_unused_html_verifier_cannot_expire_or_fence_foxglove_claim(tmp_path):
    store = _seed(tmp_path)
    remote = Remote()
    clock = Clock()
    second_outcomes = []

    def verifier(url, size, sha256):
        clock.current += timedelta(seconds=91)
        second, _remote, _clock = _dispatcher(
            tmp_path,
            remote=remote,
            clock=clock,
            verifier=_verified_report,
            lease_owner="worker-2",
        )
        second_outcomes.append(second.dispatch_one())
        return _verified_report(url, size, sha256)

    first, _remote, _clock = _dispatcher(
        tmp_path,
        remote=remote,
        clock=clock,
        verifier=verifier,
        lease_owner="worker-1",
    )

    first_outcome = first.dispatch_one()

    assert first_outcome.status == "succeeded"
    assert first.stats.lease_lost == 0
    assert first.stats.retried == first.stats.quarantined == 0
    assert second_outcomes == []
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


def test_html_http_network_failure_does_not_block_foxglove_comment(tmp_path):
    store = _seed(tmp_path)
    dispatcher, remote, _clock = _dispatcher(
        tmp_path,
        verifier=lambda *_args: {
            "success": False,
            "error_code": "report_http_unavailable",
        },
    )
    outcome = dispatcher.dispatch_one()
    assert outcome.status == "succeeded"
    assert remote.add_calls == 1
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


def test_html_http_hash_mismatch_does_not_block_foxglove_comment(tmp_path):
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
    assert outcome.status == "succeeded"
    assert remote.add_calls == 1
    assert store.list_rows("rca_delivery_jobs")[0]["status"] == "delivered"


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
    assert adapter.add_comment("t03o4q", "7041712812", "content")["remote_id"] == "c-2"
    assert [call[:2] for call in calls] == [
        ["comment", "list"],
        ["comment", "add"],
    ]
    assert all("--project-key" in call and "--work-item-id" in call for call in calls)
    assert calls[0][calls[0].index("--page-num") + 1] == "1"
    assert "--content" not in calls[0]
    assert "--content" in calls[1]


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
    result = adapter.add_comment("t03o4q", "7041712812", "content")
    assert result["success"] is False
    assert result["outcome_uncertain"] is True
    assert result["error_code"] == "feishu_add_remote_id_missing"


def test_meegle_adapter_reads_and_updates_only_attribution_fields():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return 0, json.dumps({
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
            }), ""
        if args[:2] == ["workitem", "update"]:
            return 0, json.dumps({"updated": True}), ""
        raise AssertionError(args)

    adapter = MeegleIssueCommentAdapter(runner)
    fields = adapter.get_fields(
        "t03o4q",
        "7041712812",
        ("field_9193cb", "field_8c912e"),
    )
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
            return 0, json.dumps({
                "work_item_fields": [
                    {"key": "field_9193cb", "value": "root cause"},
                    {
                        "key": "field_8c912e",
                        "value": {"link": "https://rca.example/report/index.html"},
                    },
                ]
            }), ""
        if args[:2] == ["comment", "list"]:
            page = int(args[args.index("--page-num") + 1])
            if page == 1:
                return 0, json.dumps({
                    "comments": [
                        {"comment_id": "c-unrelated", "content": "full unrelated body"}
                    ],
                    "has_more": True,
                }), ""
            return 0, json.dumps({
                "comments": [
                    {
                        "comment_id": "c-rca",
                        "content": f"canonical report\n{marker}\nfull tail",
                    }
                ],
                "has_more": False,
            }), ""
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
            return 0, json.dumps({
                "work_item_fields": [
                    {"key": "field_9193cb", "value": "root cause"},
                    {"key": "field_8c912e", "value": "https://rca.example/report"},
                ]
            }), ""
        if args[:2] == ["comment", "list"]:
            return 1, "", "permission denied"
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields_and_comments(
        "68ef617fb371dc80a10641f7", "7051585084"
    )

    assert result["success"] is False
    assert result["error_code"] == "feishu_permission_denied"
    assert "fields" not in result


def test_meegle_adapter_allows_terminal_result_only_but_never_report_only():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return 0, json.dumps({
                "work_item_fields": [
                    {"key": "field_9193cb", "value": ""},
                ]
            }), ""
        if args[:2] == ["workitem", "update"]:
            return 0, json.dumps({"updated": True}), ""
        raise AssertionError(args)

    adapter = MeegleIssueCommentAdapter(runner)
    assert adapter.get_fields(
        "t03o4q", "7041712812", ("field_9193cb",)
    ) == {"success": True, "fields": {"field_9193cb": ""}}
    assert adapter.update_fields(
        "t03o4q",
        "7041712812",
        (("field_9193cb", "自动归因未完成（非归因结论）"),),
    ) == {"success": True}
    assert adapter.update_fields(
        "t03o4q",
        "7041712812",
        (("field_8c912e", "https://invalid.example/report"),),
    )["error_code"] == "feishu_field_allowlist_invalid"
    assert [item[:2] for item in calls] == [
        ["workitem", "get"],
        ["workitem", "update"],
    ]


def test_terminal_result_only_accepts_omitted_empty_field_with_full_metadata():
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["workitem", "get"]:
            return 0, json.dumps({
                "work_item_attribute": {
                    "work_item_id": "7041712812",
                    "work_item_type": {"key": "issue", "name": "Issue"},
                }
            }), ""
        if args[:2] == ["workitem", "meta-fields"]:
            return 0, json.dumps({
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
            }), ""
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
            return 0, json.dumps({
                "work_item_attribute": {
                    "work_item_id": "7041712812",
                    "work_item_type": {"key": "issue", "name": "Issue"},
                }
            }), ""
        if args[:2] == ["workitem", "meta-fields"]:
            return 0, json.dumps({
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
            }), ""
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
            return 0, json.dumps({
                "work_item_attribute": {
                    "work_item_id": "7041712812",
                    "work_item_type": {"key": "issue", "name": "Issue"},
                }
            }), ""
        if args[:2] == ["workitem", "meta-fields"]:
            return 0, json.dumps({
                "list": [{
                    "field_key": "field_9193cb",
                    "field_name": "归因结果",
                    "field_type": "text",
                }]
            }), ""
        raise AssertionError(args)

    result = MeegleIssueCommentAdapter(runner).get_fields(
        "t03o4q",
        "7041712812",
        ("field_9193cb", "field_8c912e"),
    )

    assert result["success"] is False
    assert result["permanent"] is True
    assert result["error_code"] == "feishu_field_metadata_invalid"


def test_default_report_verifier_performs_bounded_head_then_get(monkeypatch):
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
        "https://viewer.internal/G1Q3_RCA/cases/"
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
    assert payload["ProgramArguments"][-1].endswith(
        "/scripts/pnc_rca_delivery_dispatcher.py"
    )
    environment = payload["EnvironmentVariables"]
    assert environment["HERMES_HOME"] == "/Users/songying/.hermes"
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert not any(
        token in key.upper()
        for key in environment
        for token in ("PASSWORD", "SECRET", "TOKEN", "COOKIE", "KAFKA")
    )
