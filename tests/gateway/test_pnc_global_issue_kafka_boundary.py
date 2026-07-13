"""Source-neutral RCA policy boundary for Feishu Project issue URLs."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from gateway import run as gateway_run
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.pnc_group_binding import evaluate_pnc_group_request
from gateway.session import SessionSource


ISSUE_ID = "7008267126"
ISSUE_URL = f"https://project.feishu.cn/t03o4q/issue/detail/{ISSUE_ID}"
PROJECT_KEY = "project-key"
WORK_ITEM_TYPE_KEY = "problem-type"


@pytest.fixture(autouse=True)
def _exact_status_policy_env(monkeypatch):
    monkeypatch.setenv(
        "HERMES_RCA_KAFKA_TOPIC", "feishu-project-workflow-event"
    )
    monkeypatch.setenv("HERMES_RCA_KAFKA_CREATION_RULE_VERSION", "issue-created-v1")
    monkeypatch.setenv("HERMES_RCA_KAFKA_PROJECT_KEYS", PROJECT_KEY)
    monkeypatch.setenv("HERMES_RCA_KAFKA_PROJECT_SIMPLE_NAMES", "t03o4q")
    monkeypatch.setenv(
        "HERMES_RCA_KAFKA_WORK_ITEM_TYPE_KEYS", WORK_ITEM_TYPE_KEY
    )
    monkeypatch.setenv("HERMES_RCA_KAFKA_STATUS_CHANGE_TYPES", "Reached")
    monkeypatch.setenv(
        "HERMES_RCA_KAFKA_STATE_TRANSITIONS_JSON",
        json.dumps(
            [{"state_key": "new-problem-state", "pre_status": 1, "cur_status": 2}]
        ),
    )


def _runner():
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner.config = SimpleNamespace()
    runner.session_store = SimpleNamespace()
    runner.pairing_store = SimpleNamespace()
    return runner


def _event(
    text: str,
    *,
    chat_id: str = "oc_unrelated_authorized_group",
    chat_type: str = "group",
    metadata: dict | None = None,
) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.FEISHU,
            user_id="ou_authorized",
            user_name="测试用户",
            chat_id=chat_id,
            chat_name="授权会话",
            chat_type=chat_type,
            thread_id="topic:om_issue",
            message_id="om_issue",
        ),
        message_id="om_issue",
        metadata=metadata or {},
    )


def _write_control_db(path) -> str:
    task_id = "g1q3-rca-s1-" + "a" * 64
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE business_triggers (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                submission_key TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                work_item_type_key TEXT NOT NULL,
                state TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE rca_outbox (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT,
                last_error_code TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "business-1",
                1,
                task_id,
                ISSUE_ID,
                PROJECT_KEY,
                WORK_ITEM_TYPE_KEY,
                "submitted",
                "feishu-project-workflow-event:0:42",
                "2026-07-11T00:00:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO rca_outbox VALUES (?, ?, ?, ?, ?, ?)",
            (
                "business-1",
                1,
                "completed",
                json.dumps(
                    {
                        "task_id": task_id,
                        "task_state": "running",
                    }
                ),
                "",
                "2026-07-11T00:00:05+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return task_id


def _write_manual_control_db(path) -> str:
    task_id = "g1q3-rca-s1-" + "b" * 64
    source_id = "g1q3-rca-source-v1-" + "c" * 64
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE business_triggers (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                submission_key TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                work_item_type_key TEXT NOT NULL,
                state TEXT NOT NULL,
                origin_source_id TEXT,
                source_event_id TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE rca_outbox (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT,
                last_error_code TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE rca_trigger_sources (
                source_id TEXT PRIMARY KEY,
                source_kind TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO rca_trigger_sources VALUES (?, ?)",
            (source_id, "feishu_group_manual"),
        )
        conn.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "business-manual",
                1,
                task_id,
                ISSUE_ID,
                PROJECT_KEY,
                WORK_ITEM_TYPE_KEY,
                "submitted",
                source_id,
                None,
                "2026-07-11T00:00:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO rca_outbox VALUES (?, ?, ?, ?, ?, ?)",
            (
                "business-manual",
                1,
                "completed",
                json.dumps({"task_id": task_id, "task_state": "running"}),
                "",
                "2026-07-11T00:00:05+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return task_id


@pytest.mark.parametrize(
    "chat_id",
    [
        "oc_unrelated_authorized_group",
        "ou_authorized_dm",
    ],
)
def test_explicit_issue_url_is_globally_reserved_for_kafka_status(chat_id):
    decision = evaluate_pnc_group_request(
        platform="feishu",
        chat_id=chat_id,
        text=f"请回放 mcap 并分析 {ISSUE_URL}",
    )

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_status_check"
    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.risk_gate == "kafka_only_read_only"
    assert decision.handoff_contract == {
        "contract_version": "g1q3_rca_kafka_issue_status_v1",
        "case_id": "",
        "work_item_id": ISSUE_ID,
        "issue_url": ISSUE_URL,
        "project_simple_name": "t03o4q",
        "issue_identity_source": "message",
        "source_kind": "feishu_issue_url",
        "intake": "kafka_workflow_event",
        "read_only": True,
        "group_response_cap": "L1",
    }


@pytest.mark.parametrize(
    "chat_id",
    [gateway_run.G1Q3_RCA_GROUP_ID, gateway_run.PNC_ALL_BUSINESS_TEST_GROUP_ID],
)
def test_explicit_analysis_in_fixed_groups_routes_manual_control_plane(chat_id):
    decision = evaluate_pnc_group_request(
        platform="feishu",
        chat_id=chat_id,
        text=f"请分析 {ISSUE_URL}",
        manual_mention_directed=True,
    )
    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_issue_url_on_non_feishu_platform_is_not_claimed():
    decision = evaluate_pnc_group_request(
        platform="telegram",
        chat_id="telegram-chat",
        text=ISSUE_URL,
    )
    assert decision.decision == "allow"
    assert decision.group_binding_id is None


def test_gateway_integration_tools_classifier_cannot_claim_issue_url(monkeypatch):
    source = _event(
        f"请回放 mcap 并分析 {ISSUE_URL}",
        chat_id="oc_integration_tools",
    ).source
    monkeypatch.setattr(
        gateway_run,
        "_integration_tools_config",
        lambda: {
            "enabled": True,
            "intake_chat_ids": ["oc_integration_tools"],
        },
    )

    assert gateway_run._is_integration_tools_intake_event(
        source,
        f"请回放 mcap 并分析 {ISSUE_URL}",
    ) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("chat_id", "chat_type"),
    [
        ("oc_unrelated_authorized_group", "group"),
        ("ou_authorized_dm", "dm"),
    ],
)
async def test_issue_url_without_kafka_task_waits_without_agent_or_submit(
    monkeypatch, tmp_path, chat_id, chat_type
):
    runner = _runner()
    event = _event(
        f"请分析这个问题 {ISSUE_URL}",
        chat_id=chat_id,
        chat_type=chat_type,
    )
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("issue URL status must never submit"),
    )

    async def _no_agent(*_args, **_kwargs):
        pytest.fail("issue URL status must never enter generic Agent")

    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_handle_message_with_agent",
        _no_agent,
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "尚未查询到对应 RCA 任务" in response
    assert "等待 creation event" in response
    assert "不会手工创建、重跑任务或进入通用 Agent" in response
    assert event.metadata["pnc_kafka_issue_status"] == {
        "work_item_id": ISSUE_ID,
        "read_only": True,
        "intake": "kafka_workflow_event",
        "identity_resolved": True,
        "found": False,
        "task_id": "",
    }


@pytest.mark.asyncio
async def test_metadata_only_issue_url_with_integration_keywords_is_read_only(
    monkeypatch, tmp_path
):
    runner = _runner()
    event = _event(
        "请回放这个 mcap 并分析问题卡片",
        chat_id="oc_integration_tools",
        metadata={"feishu": {"link_urls": [ISSUE_URL + "?openScene=4"]}},
    )
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_integration_tools_intake_handoff",
        lambda **_kwargs: pytest.fail("integration-tools must not claim an issue URL"),
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("issue URL status must never submit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "等待 creation event" in response
    assert event.metadata["pnc_group_binding"]["route_surface"] == (
        "rca_kafka_issue_status"
    )
    assert "integration_tools_handoff" not in event.metadata


def test_kafka_status_lookup_is_read_only_and_returns_canonical_task(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "control.sqlite3"
    task_id = _write_control_db(db_path)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    monkeypatch.setenv("HERMES_RCA_KAFKA_CONTROL_DB_PATH", str(db_path))

    task = gateway_run._find_g1q3_rca_task_by_issue_identity(
        PROJECT_KEY, WORK_ITEM_TYPE_KEY, ISSUE_ID
    )

    assert task["task_id"] == task_id
    assert task["task_state"] == "running"
    assert task["source_kind"] == "kafka_workflow_event"
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before


def test_manual_origin_status_lookup_is_read_only_and_source_neutral(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "control.sqlite3"
    task_id = _write_manual_control_db(db_path)
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    monkeypatch.setenv("HERMES_RCA_KAFKA_CONTROL_DB_PATH", str(db_path))

    task = gateway_run._find_g1q3_rca_task_by_issue_identity(
        PROJECT_KEY, WORK_ITEM_TYPE_KEY, ISSUE_ID
    )
    response = gateway_run._format_g1q3_kafka_issue_status(ISSUE_ID, task)

    assert task["task_id"] == task_id
    assert task["source_kind"] == "feishu_group_manual"
    assert task["source_event_id"] == ""
    assert "固定群手工 RCA 任务已查询到" in response
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before


def test_status_lookup_uses_full_issue_identity_when_numeric_ids_collide(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "control.sqlite3"
    expected_task_id = _write_control_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "other-business",
                9,
                "g1q3-rca-s9-" + "d" * 64,
                ISSUE_ID,
                "other-project",
                "other-type",
                "submitted",
                "other-event:0:1",
                "2026-07-11T00:01:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO rca_outbox VALUES (?, ?, ?, ?, ?, ?)",
            (
                "other-business",
                9,
                "completed",
                json.dumps({"task_id": "wrong-task", "task_state": "done"}),
                "",
                "2026-07-11T00:01:05+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setenv("HERMES_RCA_KAFKA_CONTROL_DB_PATH", str(db_path))

    task = gateway_run._find_g1q3_rca_task_by_issue_identity(
        PROJECT_KEY, WORK_ITEM_TYPE_KEY, ISSUE_ID
    )

    assert task["task_id"] == expected_task_id
    assert task["project_key"] == PROJECT_KEY
    assert task["work_item_type_key"] == WORK_ITEM_TYPE_KEY


def test_status_lookup_fails_closed_when_exact_identity_has_multiple_chains(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "control.sqlite3"
    _write_control_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO business_triggers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "conflicting-business",
                2,
                "g1q3-rca-s2-" + "e" * 64,
                ISSUE_ID,
                PROJECT_KEY,
                WORK_ITEM_TYPE_KEY,
                "submitted",
                "conflicting-event:0:2",
                "2026-07-11T00:02:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO rca_outbox VALUES (?, ?, ?, ?, ?, ?)",
            (
                "conflicting-business",
                2,
                "completed",
                json.dumps({"task_id": "conflicting-task"}),
                "",
                "2026-07-11T00:02:05+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setenv("HERMES_RCA_KAFKA_CONTROL_DB_PATH", str(db_path))

    task = gateway_run._find_g1q3_rca_task_by_issue_identity(
        PROJECT_KEY, WORK_ITEM_TYPE_KEY, ISSUE_ID
    )

    assert task is None


@pytest.mark.asyncio
async def test_existing_kafka_task_returns_status_without_submit(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "control.sqlite3"
    task_id = _write_control_db(db_path)
    runner = _runner()
    event = _event(f"分析这个问题 {ISSUE_URL}")
    monkeypatch.setenv("HERMES_RCA_KAFKA_CONTROL_DB_PATH", str(db_path))
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        gateway_run,
        "_submit_g1q3_rca_status_handoff",
        lambda **_kwargs: pytest.fail("existing Kafka task lookup must not submit"),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "Kafka RCA 任务已查询到" in response
    assert f"追踪号：{task_id}" in response
    assert "当前状态：running" in response
    assert "不会手工创建、重跑任务或进入通用 Agent" in response


@pytest.mark.asyncio
async def test_global_issue_policy_error_fails_closed_in_dm(monkeypatch):
    from gateway import pnc_group_binding

    runner = _runner()
    event = _event(ISSUE_URL, chat_id="ou_authorized_dm", chat_type="dm")
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    monkeypatch.setattr(
        pnc_group_binding,
        "evaluate_pnc_group_request",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("policy unavailable")),
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert "路由策略暂时不可用" in response
    assert "不会进入通用 Agent 或创建 VM 任务" in response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("chat_id", "chat_type"),
    [("oc_unrelated_authorized_group", "group"), ("ou_authorized_dm", "dm")],
)
async def test_non_issue_message_behavior_is_unchanged(
    monkeypatch, chat_id, chat_type
):
    runner = _runner()
    event = _event("今天下午谁有空看下普通问题", chat_id=chat_id, chat_type=chat_type)
    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_is_user_authorized",
        lambda self, source: True,
    )
    agent_calls = []

    async def _agent(self, event, source=None, quick_key=None, run_generation=None):
        agent_calls.append(event.text)
        return "generic-response"

    monkeypatch.setattr(
        gateway_run.GatewayRunner,
        "_handle_message_with_agent",
        _agent,
    )

    response = await gateway_run.GatewayRunner._handle_message(runner, event)

    assert response == "generic-response"
    assert agent_calls == ["今天下午谁有空看下普通问题"]
    assert "pnc_group_binding" not in event.metadata
