"""Offline tests for the lark-cli user-identity Aily Agent smoke."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "feishu_aily_agent_user_smoke.py"
DOC = Path(__file__).parents[2] / "docs" / "feishu-aily-agent.md"
TEST_REPORT_DOC = Path(__file__).parents[2] / "docs" / "feishu-aily-agent-test-report.md"
ROUTING_DOC = Path(__file__).parents[2] / "docs" / "hermes-knowledge-retrieval-routing.md"
WORKLOG_DOC = (
    Path(__file__).parents[2]
    / "docs"
    / "feishu-aily-business-integration-worklog.md"
)
HANDOFF_DOC = (
    Path(__file__).parents[2]
    / "docs"
    / "feishu-aily-business-integration-handoff.md"
)
SPEC = importlib.util.spec_from_file_location("feishu_aily_agent_user_smoke", SCRIPT)
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


def test_documented_smoke_keeps_question_out_of_shell_arguments():
    documentation = DOC.read_text(encoding="utf-8")

    assert "--question-stdin" in documentation
    assert "--env-file /Users/songying/.hermes/.env" in documentation
    assert "printf '%s'" not in documentation
    assert "source /Users/songying/.hermes/.env" not in documentation


def test_documented_rca_observer_is_explicitly_host_only_and_not_a_tool():
    documentation = DOC.read_text(encoding="utf-8")

    assert "仅本机 Hermes host 的 RCA observer" in documentation
    assert "可复用胡子豪" in documentation
    assert "固定 UAT" in documentation
    assert "`accepted/deferred`" in documentation
    assert "不得成为普通 Hermes/飞书对话" in documentation
    assert "不注册到通用模型 tool schema" in documentation
    assert "业务仓、VM、业务" in documentation
    assert "schema/产物" in documentation


def test_documentation_separates_verified_tool_from_future_automatic_routing():
    main_documentation = DOC.read_text(encoding="utf-8")
    test_report = TEST_REPORT_DOC.read_text(encoding="utf-8")
    routing = ROUTING_DOC.read_text(encoding="utf-8")
    worklog = WORKLOG_DOC.read_text(encoding="utf-8")
    handoff = HANDOFF_DOC.read_text(encoding="utf-8")

    assert "feishu-aily-agent-test-report.md" in main_documentation
    assert "hermes-knowledge-retrieval-routing.md" in main_documentation
    assert "本机手动 shadow planner 原型文件已存在" in main_documentation
    assert "不能把结果回灌给正在执行的 RCA" in main_documentation
    assert "answer_available=true" in main_documentation
    assert "不等于企业知识已命中" in main_documentation
    assert "active gateway" in main_documentation
    assert "395 passed" in test_report and "250 个相关测试" in test_report
    assert "149 passed" in test_report
    assert "尚未部署到 production gateway" in test_report
    assert "确定性 trigger 尚未接入自动消费" in test_report
    assert "a1e4565ec7 -> e25ba59684 -> 4a0adaba91" in test_report
    assert "HTTP 200 + 业务码 `2320008`" in test_report
    assert "session-derived attestation" in test_report
    assert "Web Search 和本地知识检索一样" in routing
    assert "版本化注册表命中的业务术语" in routing
    assert "stdin 提供的显式有界业务查询" in routing
    assert "task_type=rca` 本身不触发" in routing
    assert "无信号规范化为 `not_required`" in routing
    assert "原型可显示别名 `not_triggered`" in routing
    assert "不调用 Aily" in routing
    assert "不为内部含义\n   降级到 Web" in routing or "不调用 Aily 或 Web" in routing
    assert "内部词未命中 business 时不 fallback Web" in routing
    assert "business_knowledge_context_v1" in routing
    assert "answer_only" in routing
    assert "identity_unavailable" in routing
    assert "胡子豪固定 UAT" in routing
    assert "Keychain/lark-cli broker" in routing
    assert "不能通用借权" in routing
    assert "关闭公开网络" in routing
    assert "继续原链" in routing
    assert "host-mediated" in routing
    assert "sealed report" in routing
    for stage in (
        "盲区扫描",
        "先出原型",
        "反向采访",
        "给参照物",
        "实施计划",
        "Log 笔记",
        "交接文档",
        "出题考你",
    ):
        assert stage in worklog
    assert "不是运行时流程" in worklog
    assert "证据 Quiz" in handoff
    assert "本机启用前逐题提供代码、测试或 owner-only receipt" in handoff
    assert "不是当前上线门" in handoff

    contract_text = routing.split(
        "<!-- knowledge-routing-contract:begin -->", 1
    )[1].split("<!-- knowledge-routing-contract:end -->", 1)[0]
    contract = json.loads(contract_text.split("```json", 1)[1].split("```", 1)[0])
    assert contract["requirements"] == ["none", "auto", "required"]
    assert contract["canonical_json_v1"] == {
        "encoding": "utf-8",
        "ensure_ascii": False,
        "sort_keys": True,
        "separators": [",", ":"],
        "allow_nan": False,
    }
    assert contract["statuses"] == [
        "grounded_match",
        "answer_only",
        "no_match",
        "identity_unavailable",
        "timeout",
        "error",
        "not_required",
    ]
    assert contract["enhancement_only"] is True
    assert contract["gates_original_chain"] is False
    assert contract["failure_policy"] == "continue_original_chain"
    assert contract["business_knowledge_is_execution_evidence"] is False
    assert contract["local_host_only"] is True
    assert contract["business_repo_changes"] is False
    assert contract["vm_changes"] is False
    assert contract["credentials_and_provider_host_only"] is True
    assert contract["owner_only_reference"] is True
    assert contract["web_fallback_for_business"] is False
    assert contract["identity_strategy"] == (
        "reuse_pre_registered_huzhihao_fixed_uat"
    )
    assert contract["capability_priority"] is True
    assert contract["risk_deferred"] is True
    assert contract["deferred_items"] == [
        "dedicated_service_identity",
        "identity_and_delegation_risk_review",
    ]
    assert contract["identity_controls"] == {
        "pre_registered_owner": "胡子豪",
        "exact_match_required": ["profile", "open_id", "union_id"],
        "verify_before_each_create": True,
        "credential_broker": "keychain_lark_cli",
        "raw_token_export": False,
        "token_to_vm": False,
        "rca_host_observer_provider_only": True,
        "general_session_delegation": False,
    }
    assert contract["initial_runtime_scope"] == (
        "rca_local_host_observer_provider_only"
    )
    assert contract["ordinary_task_provider_enabled"] is False
    assert contract["trigger_policy"] == {
        "type": "deterministic_registered_term_or_explicit_query",
        "signals": ["registered_business_term", "explicit_bounded_query"],
        "rca_task_type_alone_triggers": False,
        "no_signal_requirement": "none",
        "no_signal_status": "not_required",
        "prototype_status_alias": "not_triggered",
        "provider_called_without_signal": False,
        "web_fallback_without_signal": False,
    }
    assert contract["manual_shadow_prototype"] == {
        "script": "~/.hermes/scripts/pnc_rca_aily_shadow_observer.py",
        "wrapper": "~/bin/pnc-rca-aily-shadow",
        "execution": "manual_one_shot",
        "enabled": False,
        "registered": False,
        "daemon": False,
        "inspect_network": False,
        "dry_run_network": False,
        "real_provider_enabled": False,
        "durable_create_poll_recovery": False,
        "consumer_seam_enabled": False,
    }
    assert contract["original_chain_mutations"] == {
        "dispatcher": False,
        "execution_request_v2": False,
        "vm_goal": False,
        "core_result": False,
        "required_delivery": False,
    }
    assert contract["design_method_runtime"] is False
    assert contract["effective_influence_by_mode"] == {
        "shadow": {
            "grounded_match": "observe_only",
            "answer_only": "observe_only",
            "no_match": "none",
            "identity_unavailable": "none",
            "timeout": "none",
            "error": "none",
            "not_required": "none",
        },
        "active": {
            "grounded_match": "reference_only",
            "answer_only": "reference_only",
            "no_match": "none",
            "identity_unavailable": "none",
            "timeout": "none",
            "error": "none",
            "not_required": "none",
        },
    }
    assert contract["lookup_unique_key"] == [
        "submission_key",
        "generation",
        "phase",
        "query_hmac_sha256",
        "provider_policy_fingerprint",
        "identity_policy_fingerprint",
    ]
    assert contract["rca_enhancement_points"] == [
        "post_vm_materialization_host_async_preflight",
        "post_completed_sealed_report_host_async_followup",
        "host_private_owner_only_reference",
    ]
    assert contract["preflight_task_states"] == [
        "pending",
        "claimed",
        "running",
        "in_progress",
        "completed",
        "done",
        "closed",
    ]
    assert contract["report_followup_task_states"] == [
        "completed",
        "done",
        "closed",
    ]
    assert contract["report_followup_requires_sealed_delivery"] is True
    assert contract["rca_phases"] == ["preflight", "report_followup:1"]
    assert contract["second_stage_host_observer"] == {
        "trigger": "completed_sealed_report",
        "source_access": "read_only",
        "term_extraction": "deterministic_registry",
        "provider_execution": "local_host_only",
        "output": "host_private_owner_only_addendum",
        "original_task_wait": False,
    }
    assert contract["second_stage_controls"] == {
        "sealed_report_required": True,
        "same_generation_binding": True,
        "cross_generation_reuse": False,
        "human_blocking_state": False,
        "main_task_wait": False,
        "main_task_resume": False,
        "required_delivery_effect": False,
        "business_schema_write": False,
        "report_observation_failure_blocks_main": False,
        "max_rounds": 1,
        "max_queries": 2,
    }
    assert contract["rca_provider_session_policy"] == {
        "create_request_session_id": None,
        "fresh_session_per_job": True,
        "cross_job_session_reuse": False,
    }
    assert {
        "schema_version",
        "submission_key",
        "generation",
        "phase",
        "mode",
        "status",
        "influence",
        "rca_contract_sha256",
        "query_hmac_sha256",
        "provider_policy_fingerprint",
        "identity_policy_fingerprint",
        "retrieved_at",
        "latency_ms",
    } == set(contract["lookup_receipt_common_required"])
    assert contract["lookup_receipt_status_fields_exact"] == {
        "grounded_match": [
            "answer_sha256",
            "answer_bytes",
            "source_refs_relpath",
            "source_refs_sha256",
            "retrieval_activity_receipt_relpath",
            "retrieval_activity_receipt_sha256",
        ],
        "answer_only": ["answer_sha256", "answer_bytes"],
        "no_match": [
            "provider_no_hit_receipt_relpath",
            "provider_no_hit_receipt_sha256",
        ],
        "identity_unavailable": ["error_code"],
        "timeout": ["error_code"],
        "error": ["error_code"],
    }
    assert contract["failure_statuses"] == [
        "identity_unavailable",
        "timeout",
        "error",
    ]
    assert set(contract["consumer_receipt_binding_required"]) == {
        "lookup_receipt_relpath",
        "lookup_receipt_sha256",
    }
    consumer_equal_fields = {
        "submission_key",
        "generation",
        "phase",
        "mode",
        "status",
        "influence",
        "rca_contract_sha256",
        "query_hmac_sha256",
        "answer_sha256",
        "answer_bytes",
        "provider_policy_fingerprint",
        "identity_policy_fingerprint",
    }
    assert set(contract["consumer_lookup_fields_must_equal"]) == (
        consumer_equal_fields
    )
    assert contract["consumer_content_binding"] == {
        "business_knowledge_context_v1": "answer",
        "business_knowledge_addendum_v1": "content",
        "encoding": "utf-8",
        "length_field": "answer_bytes",
        "sha256_field": "answer_sha256",
    }
    host_audit_common = set(contract["host_audit_receipt_common_fields_exact"])
    assert host_audit_common == {
        "schema_version",
        "submission_key",
        "generation",
        "phase",
        "mode",
        "status",
        "influence",
        "query_hmac_sha256",
        "provider_policy_fingerprint",
        "identity_policy_fingerprint",
        "lookup_receipt_sha256",
        "retrieved_at",
        "latency_ms",
    }
    assert contract["host_audit_receipt_status_fields_exact"] == {
        "grounded_match": ["answer_bytes"],
        "answer_only": ["answer_bytes"],
        "no_match": [],
        "identity_unavailable": ["error_code"],
        "timeout": ["error_code"],
        "error": ["error_code"],
    }
    host_audit_forbidden = contract["host_audit_receipt_forbidden_fields"]
    assert "summary" in host_audit_forbidden
    assert "query" in host_audit_forbidden
    assert "agent_chat_id" in host_audit_forbidden
    assert "lookup_receipt_relpath" in host_audit_forbidden
    assert "source_refs_relpath" in host_audit_forbidden
    assert "## 从检索到交付的八步闭环" not in routing
    assert "### 闭环状态机" not in routing
    for obsolete_contract in (
        "business_knowledge_gap_v1",
        "second_stage_fork",
        "second_stage_gap_outcomes",
        "valid_gap_atomic_seal",
        "gateway/pnc_rca_stage_lineage.py",
        "<artifact_root>/business_knowledge/gaps/gap-1.json",
        '"phase": "gap:1"',
        "专用 RCA 服务用户/UAT",
        "绝不借用固定员工 UAT",
        "不用该 UAT",
        "正式 RCA 分支回归",
    ):
        assert obsolete_contract not in routing

    receipt_section = routing.split("`answer_only` receipt 示例：", 1)[1]
    receipt = json.loads(
        receipt_section.split("```json", 1)[1].split("```", 1)[0]
    )
    assert receipt["schema_version"] == "business_knowledge_lookup_receipt_v1"
    assert receipt["mode"] == "active"
    assert receipt["status"] == "answer_only"
    assert receipt["influence"] == "reference_only"
    for field in contract["lookup_receipt_common_required"]:
        assert receipt[field] or receipt[field] == 0
    for field in contract["lookup_receipt_status_fields_exact"]["answer_only"]:
        assert receipt[field]
    assert receipt["answer_bytes"] > 0
    assert set(receipt) == set(contract["lookup_receipt_common_required"]) | set(
        contract["lookup_receipt_status_fields_exact"]["answer_only"]
    )

    context_section = routing.split(
        "成功返回正文的查询再生成 owner-only `business_knowledge_context_v1`：", 1
    )[1]
    context = json.loads(
        context_section.split("```json", 1)[1].split("```", 1)[0]
    )
    assert context["mode"] == "active"
    assert context["status"] == "answer_only"
    assert context["influence"] == "reference_only"
    assert context["submission_key"]
    assert context["phase"] == "preflight"
    assert context["rca_contract_sha256"]
    for field in contract["consumer_receipt_binding_required"]:
        assert context[field]
    for field in contract["consumer_lookup_fields_must_equal"]:
        assert context[field] == receipt[field]
    context_bytes = context["answer"].encode("utf-8")
    assert len(context_bytes) == context["answer_bytes"]
    assert hashlib.sha256(context_bytes).hexdigest() == context["answer_sha256"]
    tampered_context_bytes = (context["answer"] + "x").encode("utf-8")
    assert not (
        len(tampered_context_bytes) == context["answer_bytes"]
        and hashlib.sha256(tampered_context_bytes).hexdigest()
        == context["answer_sha256"]
    )

    addendum_section = routing.split("Owner-only addendum 使用 exact schema：", 1)[1]
    addendum = json.loads(
        addendum_section.split("```json", 1)[1].split("```", 1)[0]
    )
    assert addendum["schema_version"] == "business_knowledge_addendum_v1"
    assert addendum["mode"] == "active"
    assert addendum["status"] == "answer_only"
    assert addendum["influence"] == "reference_only"
    assert addendum["phase"] == "report_followup:1"
    assert addendum["rca_contract_sha256"]
    assert addendum["sealed_report_sha256"]
    assert addendum["report_seal_receipt_sha256"]
    assert addendum["term_registry_fingerprint"]
    for field in contract["lookup_receipt_status_fields_exact"]["answer_only"]:
        assert addendum[field]
    for field in contract["consumer_receipt_binding_required"]:
        assert addendum[field]
    assert addendum["lookup_receipt_relpath"].startswith("report-followup-1/")
    addendum_bytes = addendum["content"].encode("utf-8")
    assert len(addendum_bytes) == addendum["answer_bytes"]
    assert hashlib.sha256(addendum_bytes).hexdigest() == addendum["answer_sha256"]

    safe_section = routing.split(
        "Host-private audit 层使用 exact "
        "`business_knowledge_safe_receipt_v1`，例如：",
        1,
    )[1]
    safe_receipt = json.loads(
        safe_section.split("```json", 1)[1].split("```", 1)[0]
    )
    safe_status_fields = set(
        contract["host_audit_receipt_status_fields_exact"][safe_receipt["status"]]
    )
    assert set(safe_receipt) == host_audit_common | safe_status_fields
    assert not (set(safe_receipt) & set(host_audit_forbidden))
    assert set({**safe_receipt, "raw_provider_payload": {}}) != (
        host_audit_common | safe_status_fields
    )

    assert "同一 `phase` 内相同 query digest 不重复调用" in routing
    assert "complete original task and required delivery without host wait" in routing
    assert "observer 只读，不创建或修改 VM" in routing
    assert "不要求业务仓库或 VM 新增 helper" in routing
    assert "现有一次性 `run_agent_chat_user()` 不能原样承担" in routing
    assert "现有 transport 的可选 `session_id` 参数" in routing
    assert "POST 前先 durable seal `creating` job state" in routing
    assert "未知 key 立即拒绝" in routing
    assert "只校验 `lookup_receipt_sha256` 而不校验正文绑定不合格" in routing
    assert "reserved -> identity_check -> identity_unavailable" in routing
    assert "这些状态以 lookup receipt 终止且不创建" in routing
    assert "最多一轮、最多两条 query" in routing
    assert "原任务不等待、不 resume、不重投递" in routing
    assert "每次 Aily create 前" in routing
    assert "profile/open_id/union_id 任一不符" in routing
    assert "每个六元 job 获得全新 Aily session" in routing
    assert "## 普通飞书任务（首版不启用）" in routing
    assert "普通飞书任务在 scope gate 直接得到 `not_required`" in routing
    assert "`not_triggered`（规范映射 `not_required`）" in routing
    assert "未启用、未注册且非 daemon" in routing
    assert "没有 consumer seam" in routing
    assert "不回灌正在执行的 RCA" in routing
    assert "问题不写入命令参数或 shell" in routing or "问题通过 stdin" in routing
    assert "真实 provider 当前禁用且不可用" in routing
    assert "base_contract_sha256" not in routing
    assert "core_result_sha256" not in routing


def test_business_integration_handoff_quiz_is_complete_and_evidence_bounded():
    handoff = HANDOFF_DOC.read_text(encoding="utf-8")
    worklog = WORKLOG_DOC.read_text(encoding="utf-8")
    quiz = handoff.split("## 证据 Quiz", 1)[1].split("## 回滚", 1)[0]

    for number in range(1, 11):
        assert f"{number}. **" in quiz
    assert "胡子豪固定 UAT" in quiz
    assert "接受并延后" in quiz
    assert "只有本机 RCA observer" in quiz
    assert "不依赖 VM gap" in quiz
    assert "owner-only reference" in quiz
    assert "业务 schema/artifact" in quiz
    assert "历史 `Completed + content` 本身不够" in quiz
    assert "无注册术语或显式查询缺失时做什么" in quiz
    assert "consumer seam" in quiz
    assert "不是运行时流程" in worklog
    assert "文档自检不代表实现验收" in worklog
    assert "不得把 Quiz 答案解释为 observer 已实现" in worklog


def _env_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / ".env"
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
    return path


def _config(tmp_path: Path, **overrides):
    values = {
        "config_dir": tmp_path,
        "profile": "cli_expected",
        "expected_app_id": "cli_expected",
        "expected_user_open_id": "ou_expected",
        "expected_user_union_id": "on_expected",
        "agent_id": "agent_expected",
    }
    values.update(overrides)
    return smoke.ProbeConfig(**values)


def _completed(payload: dict, returncode: int = 0):
    return SimpleNamespace(
        returncode=returncode,
        stdout=json.dumps(payload, ensure_ascii=False).encode(),
        stderr=b"",
    )


def _user_info(**overrides):
    data = {
        "name": "Display Name",
        "open_id": "ou_expected",
        "union_id": "on_expected",
    }
    data.update(overrides)
    return {"code": 0, "data": data}


def _create():
    return {"code": 0, "data": {"agent_chat_id": "chat_1", "session_id": "s_1"}}


def _poll(status: str, content=None):
    data = {"status": status}
    if content is not None:
        data["content"] = content
    return {"code": 0, "data": data}


def test_happy_path_uses_user_info_then_stdin_create_and_poll(tmp_path, monkeypatch):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Queued")),
            _completed(
                _poll(
                    "Completed",
                    [
                        {"type": "text", "text": "A"},
                        {"type": "text", "text": "A"},
                        {"type": "text", "text": "   \n"},
                        {"type": "text", "text": "B"},
                        {"type": "text", "text": "A"},
                        {"type": "artifact", "agent_artifact_id": "artifact_1"},
                    ],
                )
            ),
        ]
    )
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return next(responses)

    sleeps = []
    monkeypatch.setattr(smoke, "_bounded_subprocess_run", fake_run)
    monkeypatch.setattr(smoke.time, "sleep", sleeps.append)
    monkeypatch.setenv("FEISHU_AILY_USER_ACCESS_TOKEN", "must-not-forward")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "must-not-forward-either")
    monkeypatch.setenv("HERMES_HOME", "/must-not-forward-hermes")
    monkeypatch.setenv("OPENCLAW_HOME", "/must-not-forward-openclaw")
    monkeypatch.setenv("HOME", "/safe-home")
    monkeypatch.setenv("USER", "safe-user")
    monkeypatch.setenv("LOGNAME", "safe-logname")
    monkeypatch.setenv("TMPDIR", "/safe-tmp")

    result = smoke.run_probe(_config(tmp_path), "OOI是什么?")

    assert result == {
        "ok": True,
        "phase": "completed",
        "status": "Completed",
        "app_id": "cli_expected",
        "user_identity_verified": True,
        "agent_id": "agent_expected",
        "poll_count": 2,
        "answer_available": True,
        "answer_length": len("ABA"),
        "text_item_count": 3,
        "artifact_count": 1,
    }
    assert calls[0][0] == [
        "lark-cli",
        "--profile",
        "cli_expected",
        "api",
        "GET",
        "/open-apis/authen/v1/user_info",
        "--as",
        "user",
        "--format",
        "json",
    ]
    create_command, create_kwargs = calls[1]
    assert create_command == [
        "lark-cli",
        "--profile",
        "cli_expected",
        "api",
        "POST",
        "/open-apis/aily/v1/agents/agent_expected/chats",
        "--as",
        "user",
        "--data",
        "-",
        "--format",
        "json",
    ]
    assert "OOI是什么?" not in create_command
    assert json.loads(create_kwargs["input"])["user_message"]["content"] == [
        {"type": "text", "text": "OOI是什么?"}
    ]

    allowed_env = {
        smoke.CONFIG_DIR_ENV,
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "HOME",
        "USER",
        "LOGNAME",
        "TMPDIR",
    }
    for command, kwargs in calls:
        assert kwargs["env"][smoke.CONFIG_DIR_ENV] == str(tmp_path)
        assert set(kwargs["env"]) <= allowed_env
        assert "FEISHU_AILY_USER_ACCESS_TOKEN" not in kwargs["env"]
        assert "FEISHU_AILY_AUTH_APP_SECRET" not in kwargs["env"]
        assert "HERMES_HOME" not in kwargs["env"]
        assert "OPENCLAW_HOME" not in kwargs["env"]
        assert kwargs["env"]["HOME"] == "/safe-home"
        assert kwargs["env"]["USER"] == "safe-user"
        assert kwargs["env"]["LOGNAME"] == "safe-logname"
        assert kwargs["env"]["TMPDIR"] == "/safe-tmp"
        assert kwargs["cwd"] == "/"
        assert kwargs["start_new_session"] is True
        assert command[1:3] == ["--profile", "cli_expected"]
        if command[3:5] == ["api", "GET"]:
            assert command[6:8] == ["--as", "user"]
            assert kwargs["stdin"] is subprocess.DEVNULL
    assert sleeps == [1.0, 2.0]


def test_show_answer_is_explicit_and_bounded(tmp_path):
    long_text = "x" * (smoke.MAX_DISPLAY_ANSWER_CHARS + 25)
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Completed", [{"type": "text", "text": long_text}])),
        ]
    )

    result = smoke.run_probe(
        _config(tmp_path),
        "question",
        include_answer=True,
        runner=lambda _command, **_kwargs: next(responses),
    )

    assert result["answer_length"] == len(long_text)
    assert result["answer"] == long_text[: smoke.MAX_DISPLAY_ANSWER_CHARS]
    assert result["answer_truncated"] is True


def test_profile_must_equal_expected_app_id_before_runner(tmp_path):
    called = False

    def runner(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path, profile="cli_other"), "question", runner=runner
        )

    assert exc.value.payload["error"] == "--profile must equal --expected-app-id"
    assert called is False


@pytest.mark.parametrize(
    "user_info_override",
    [
        {"open_id": "ou_other"},
        {"union_id": "on_other"},
    ],
)
def test_user_info_hard_pins_open_and_union_ids_before_create(
    tmp_path, user_info_override
):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return _completed(_user_info(**user_info_override))

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "identity"
    assert len(calls) == 1
    assert not any(command[3:5] == ["api", "POST"] for command in calls)


def test_invalid_user_info_json_fails_before_create_without_echo(tmp_path):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(
            returncode=0, stdout=b"private invalid JSON", stderr=b""
        )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "command"
    assert "private" not in json.dumps(exc.value.payload)
    assert len(calls) == 1


def test_missing_user_token_fails_before_create(tmp_path):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return _completed({"ok": False, "error": {"type": "auth"}}, returncode=3)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "identity"
    assert len(calls) == 1


@pytest.mark.parametrize("terminal_status", ["Failed", "Cancelled", "Finished", "completed"])
def test_only_exact_completed_status_succeeds(tmp_path, terminal_status):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll(terminal_status, [{"type": "text", "text": "partial"}])),
        ]
    )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            runner=lambda _command, **_kwargs: next(responses),
        )

    assert exc.value.payload["phase"] == "result"
    assert exc.value.payload["status"] == terminal_status


def test_total_timeout_covers_poll_sleep(tmp_path):
    now = [0.0]
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Queued")),
        ]
    )

    def sleep(seconds):
        now[0] += seconds

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            timeout=0.5,
            runner=lambda _command, **_kwargs: next(responses),
            clock=lambda: now[0],
            sleeper=sleep,
        )

    assert exc.value.payload["phase"] == "timeout"


def test_injected_runner_timeout_does_not_echo_captured_output(tmp_path):
    def runner(command, **kwargs):
        raise subprocess.TimeoutExpired(
            command, kwargs["timeout"], output=b"private-answer", stderr=b"private-error"
        )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    serialized = json.dumps(exc.value.payload)
    assert exc.value.payload["phase"] == "timeout"
    assert "private" not in serialized


def test_injected_runner_oversized_output_is_rejected_without_echo(tmp_path):
    oversized = b"sensitive" + b"x" * smoke.MAX_COMMAND_OUTPUT_BYTES

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            runner=lambda _command, **_kwargs: SimpleNamespace(
                returncode=0, stdout=oversized, stderr=b""
            ),
        )

    assert exc.value.payload == {
        "ok": False,
        "phase": "output",
        "error": "lark-cli output exceeded the byte limit",
    }
    assert "sensitive" not in json.dumps(exc.value.payload)


def test_invalid_input_fails_before_starting_lark_cli(tmp_path):
    called = False

    def runner(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), " ", runner=runner)

    assert exc.value.payload["phase"] == "input"
    assert called is False


def test_config_dir_rejects_group_or_world_permissions(tmp_path):
    tmp_path.chmod(0o750)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=lambda *_a, **_k: None)

    assert exc.value.payload["error"] == "--config-dir permissions must be 0700"


def test_config_dir_rejects_symlink(tmp_path):
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(link),
            "question",
            runner=lambda *_args, **_kwargs: pytest.fail("runner must not start"),
        )

    assert exc.value.payload["error"] == "--config-dir must name an existing directory"


def test_config_dir_rejects_different_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(smoke.os, "getuid", lambda: tmp_path.stat().st_uid + 1)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=lambda *_a, **_k: None)

    assert exc.value.payload["error"] == "--config-dir must be owned by the current user"


def test_main_reads_question_only_from_stdin_and_hides_identity_ids(
    tmp_path, monkeypatch, capsys
):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Completed", [{"type": "text", "text": "internal"}])),
        ]
    )
    monkeypatch.setattr(
        smoke, "_bounded_subprocess_run", lambda _command, **_kwargs: next(responses)
    )
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("OOI是什么?"))

    exit_code = smoke.main(
        [
            "--config-dir",
            str(tmp_path),
            "--profile",
            "cli_expected",
            "--expected-app-id",
            "cli_expected",
            "--expected-user-open-id",
            "ou_expected",
            "--expected-user-union-id",
            "on_expected",
            "--agent-id",
            "agent_expected",
            "--question-stdin",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["answer_available"] is True
    assert output["answer_length"] == len("internal")
    assert "answer" not in output
    serialized = json.dumps(output)
    assert "ou_expected" not in serialized
    assert "on_expected" not in serialized
    assert "user_name" not in output
    assert output["user_identity_verified"] is True


def test_env_file_is_parsed_as_data_and_only_returns_allowlisted_values(tmp_path):
    marker = tmp_path / "must-not-exist"
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_expected",
                "FEISHU_AILY_AGENT_ID=agent_expected",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_expected",
                "FEISHU_AILY_USER_UNION_ID=on_expected",
                "FEISHU_AILY_AUTH_APP_SECRET=must-not-be-returned",
                "FEISHU_AILY_USER_ACCESS_TOKEN=must-not-be-returned",
                f"EVIL=$(touch {marker})",
            ]
        ),
    )

    values = smoke._load_env_file(env_file)

    assert set(values) == smoke.ENV_FILE_KEYS
    assert "must-not-be-returned" not in repr(values)
    assert not marker.exists()


def test_main_loads_env_file_and_defaults_profile_to_app_id(
    tmp_path, monkeypatch, capsys
):
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_expected",
                "FEISHU_AILY_AGENT_ID=agent_expected",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_expected",
                "FEISHU_AILY_USER_UNION_ID=on_expected",
            ]
        ),
    )
    captured = {}

    def fake_probe(config, question, **_kwargs):
        captured["config"] = config
        captured["question"] = question
        return {"ok": True, "phase": "completed"}

    monkeypatch.setattr(smoke, "run_probe", fake_probe)
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("OOI是什么?"))

    exit_code = smoke.main(
        ["--env-file", str(env_file), "--question-stdin"]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert captured["question"] == "OOI是什么?"
    assert captured["config"] == _config(tmp_path)


def test_explicit_flags_override_env_file_values(tmp_path):
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_from_env",
                "FEISHU_AILY_AGENT_ID=agent_from_env",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_from_env",
                "FEISHU_AILY_USER_UNION_ID=on_from_env",
            ]
        ),
    )
    args = smoke._parser().parse_args(
        [
            "--env-file",
            str(env_file),
            "--expected-app-id",
            "cli_explicit",
            "--agent-id",
            "agent_explicit",
            "--expected-user-open-id",
            "ou_explicit",
            "--expected-user-union-id",
            "on_explicit",
            "--question-stdin",
        ]
    )

    config = smoke._config_from_args(args)

    assert config.expected_app_id == "cli_explicit"
    assert config.profile == "cli_explicit"
    assert config.agent_id == "agent_explicit"
    assert config.expected_user_open_id == "ou_explicit"
    assert config.expected_user_union_id == "on_explicit"


@pytest.mark.parametrize(
    ("kind", "expected_error"),
    [
        ("missing", "--env-file must name an existing file"),
        ("permissions", "--env-file permissions must be 0600"),
        ("symlink", "--env-file must be a regular non-symlink file"),
    ],
)
def test_env_file_path_safety_checks(tmp_path, kind, expected_error):
    env_file = tmp_path / ".env"
    if kind == "permissions":
        env_file.write_text("FEISHU_AILY_AUTH_MODE=user\n", encoding="utf-8")
        env_file.chmod(0o640)
    elif kind == "symlink":
        target = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
        env_file = tmp_path / "linked.env"
        env_file.symlink_to(target)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == expected_error


def test_env_file_rejects_different_owner(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    monkeypatch.setattr(smoke.os, "getuid", lambda: env_file.stat().st_uid + 1)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == "--env-file must be owned by the current user"


def test_env_file_growth_after_open_is_bounded_and_rejected(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    real_read = smoke.os.read
    grew = False

    def growing_read(file_descriptor, size):
        nonlocal grew
        if not grew:
            grew = True
            with env_file.open("ab") as stream:
                stream.write(b"x" * (smoke.MAX_ENV_FILE_BYTES + 1))
        return real_read(file_descriptor, size)

    monkeypatch.setattr(smoke.os, "read", growing_read)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == "--env-file exceeds the byte limit"


def test_env_file_missing_required_setting_fails_before_probe(
    tmp_path, monkeypatch, capsys
):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    called = False

    def fake_probe(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(smoke, "run_probe", fake_probe)
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("question"))

    exit_code = smoke.main(
        ["--env-file", str(env_file), "--question-stdin"]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert output["phase"] == "input"
    assert output["error"] == "--expected-app-id is required"
    assert called is False


def test_main_bounds_stdin_before_config_or_network(monkeypatch, capsys):
    class TrackingStdin:
        def __init__(self):
            self.requested_size = None

        def read(self, size=-1):
            self.requested_size = size
            return "x" * size

    stdin = TrackingStdin()
    called = False

    def fake_probe(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(smoke.sys, "stdin", stdin)
    monkeypatch.setattr(smoke, "run_probe", fake_probe)

    exit_code = smoke.main(["--question-stdin"])

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert stdin.requested_size == smoke.MAX_QUESTION_CHARS + 1
    assert output["phase"] == "input"
    assert called is False


def test_cli_rejects_question_argument(tmp_path):
    with pytest.raises(SystemExit) as exc:
        smoke.main(
            [
                "--config-dir",
                str(tmp_path),
                "--profile",
                "cli_expected",
                "--expected-app-id",
                "cli_expected",
                "--expected-user-open-id",
                "ou_expected",
                "--expected-user-union-id",
                "on_expected",
                "--agent-id",
                "agent_expected",
                "--question",
                "must not enter argv",
            ]
        )

    assert exc.value.code == 2


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_timeout_kills_child_group_and_reaps_leader(tmp_path):
    marker = tmp_path / "child-survived"
    child_code = (
        "import pathlib,time;"
        "time.sleep(0.4);"
        f"pathlib.Path({str(marker)!r}).write_text('alive')"
    )
    parent_code = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        "time.sleep(10)"
    )
    harness = f"""
import importlib.util, pathlib, subprocess, sys, time
spec = importlib.util.spec_from_file_location('user_smoke_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', {parent_code!r}],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=0.1,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except subprocess.TimeoutExpired:
    pass
else:
    raise SystemExit(2)
time.sleep(0.5)
raise SystemExit(1 if pathlib.Path({str(marker)!r}).exists() else 0)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_timeout_when_child_does_not_read_stdin():
    harness = f"""
import importlib.util, subprocess, sys, time
spec = importlib.util.spec_from_file_location('user_smoke_stdin_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
started = time.monotonic()
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', 'import time; time.sleep(1)'],
        input=b'x' * (2 * 1024 * 1024),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=0.1,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except subprocess.TimeoutExpired:
    elapsed = time.monotonic() - started
    raise SystemExit(0 if elapsed < 0.8 else 3)
raise SystemExit(2)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_kills_process_on_oversized_output():
    harness = f"""
import importlib.util, subprocess, sys
spec = importlib.util.spec_from_file_location('user_smoke_output_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
module.MAX_COMMAND_OUTPUT_BYTES = 1024
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', "import os,time; os.write(1, b'x' * 4096); time.sleep(10)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=2,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except module.ProbeFailure as exc:
    raise SystemExit(0 if exc.payload.get('phase') == 'output' else 3)
raise SystemExit(2)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
