"""Regression tests for user-facing gateway tool progress and status formatting."""

from gateway.config import Platform
from gateway.display_config import resolve_display_setting
from gateway.feishu_reply import (
    format_feishu_lifecycle_status,
    sanitize_feishu_final_response,
    sanitize_feishu_internal_error,
    sanitize_feishu_visible_text,
)


def _format_tool_progress_message(*args, **kwargs):
    # Import lazily: gateway.run bridges the live ~/.hermes config into
    # os.environ at import time, so importing it during test collection can
    # pollute unrelated gateway tests (API_SERVER_PORT, tokens, etc.).
    from gateway.run import format_tool_progress_message

    return format_tool_progress_message(*args, **kwargs)


class TestFeishuToolProgressFormatting:
    def test_feishu_builtin_default_uses_human_progress(self):
        assert resolve_display_setting({}, "feishu", "tool_progress") == "human"

    def test_feishu_uses_human_progress_when_global_is_new(self):
        config = {"display": {"tool_progress": "new", "platforms": {}}}

        assert resolve_display_setting(config, "feishu", "tool_progress") == "human"

    def test_feishu_explicit_platform_override_can_request_raw_progress(self):
        config = {"display": {"tool_progress": "new", "platforms": {"feishu": {"tool_progress": "all"}}}}

        assert resolve_display_setting(config, "feishu", "tool_progress") == "all"

    def test_feishu_legacy_platform_override_can_request_raw_progress(self):
        config = {"display": {"tool_progress": "new", "tool_progress_overrides": {"feishu": "all"}}}

        assert resolve_display_setting(config, "feishu", "tool_progress") == "all"

    def test_feishu_global_off_remains_kill_switch(self):
        config = {"display": {"tool_progress": "off", "platforms": {}}}

        assert resolve_display_setting(config, "feishu", "tool_progress") == "off"

    def test_feishu_human_progress_hides_internal_tool_names(self):
        msg = _format_tool_progress_message(
            Platform.FEISHU,
            "human",
            "mcp_feishu_project_search_user_info",
            "王平",
            {"user_keys": ["王平"], "project_key": "zksag9"},
        )

        assert msg == "🔎 正在查询飞书项目数据..."
        assert "mcp_feishu_project" not in msg
        assert "search_user_info" not in msg
        assert "王平" not in msg

    def test_feishu_human_progress_suppresses_skill_loading(self):
        msg = _format_tool_progress_message(
            Platform.FEISHU,
            "human",
            "skill_view",
            "feishu-project",
            {"name": "feishu-project"},
        )

        assert msg is None

    def test_non_feishu_progress_keeps_existing_tool_trace(self):
        msg = _format_tool_progress_message(
            Platform.TELEGRAM,
            "new",
            "mcp_feishu_project_search_user_info",
            "王平",
            {"user_keys": ["王平"]},
        )

        assert "mcp_feishu_project_search_user_info" in msg
        assert "王平" in msg


class TestFeishuLifecycleStatusFormatting:
    def test_feishu_human_lifecycle_suppresses_retry_and_provider_chatter(self):
        noisy_messages = [
            "⏳ Retrying in 2.3s (attempt 1/3)...",
            "⏱️ Rate limited. Waiting 5.0s (attempt 2/3)...",
            "⛔ Provider lane open for 17s (custom:gpt-5.5) — trying fallback...",
            "🔄 Primary model failed — switching to fallback: gpt-5.4-mini via custom:sub2api",
            "⏳ Provider lane cooling down for 17s (custom:sub2api:gpt-5.4-mini)",
            "⚠️ Max retries (3) exhausted — trying fallback...",
            "⚠️ Non-retryable error (HTTP 400) — trying fallback...",
            "⚠️ No response from provider for 60s (model: gpt-5.5, context: ~120,000 tokens). Aborting call.",
            "⚠️ Connection to provider dropped (ReadError). Reconnecting… (attempt 2/3)",
            "🔄 Reconnected — resuming…",
            "🔌 Detected stale connections from a previous provider issue — cleaned up automatically. Proceeding with fresh connection.",
            "⚠️ Empty response from model — retrying (1/3)",
            "⚠️ Model returning empty responses — switching to fallback provider...",
            "↻ Switched to fallback: gpt-5.4-mini (custom:sub2api)",
            "↻ Provider returned an empty post-tool message — recovering with a compact continuation prompt",
            "↻ Thinking-only response — prefilling to continue (1/2)",
        ]

        for raw in noisy_messages:
            assert format_feishu_lifecycle_status(
                Platform.FEISHU,
                "human",
                "lifecycle",
                raw,
            ) is None

    def test_feishu_human_lifecycle_humanizes_final_model_failure(self):
        final_failures = [
            "❌ API failed after 3 retries — HTTP 502: Upstream request failed",
            "❌ Rate limited after 3 retries — HTTP 429: Too Many Requests",
            "❌ Connection to provider failed after 3 attempts. The provider may be experiencing issues — ReadError",
            "❌ Non-retryable error (HTTP 400): BadRequestError: invalid reasoning_content",
            "❌ Model returned no content after all retries and fallback attempts.",
        ]

        for raw in final_failures:
            msg = format_feishu_lifecycle_status(
                Platform.FEISHU,
                "human",
                "lifecycle",
                raw,
            )

            assert msg == (
                "这次模型服务没有正常返回，我这边已经自动重试过了。\n"
                "可以稍后再试一次；如果连续出现，我再继续排网关和上游链路。"
            )
            assert "HTTP" not in msg
            assert "Provider" not in msg
            assert "provider" not in msg
            assert "retries" not in msg

    def test_feishu_human_final_response_humanizes_api_retry_failure(self):
        final_responses = [
            "API call failed after 3 retries: HTTP 502: Upstream request failed",
            "Error: API call failed after 3 retries: HTTP 503: provider overloaded",
            "❌ Model returned no content after all retries and fallback attempts.",
            "❌ Non-retryable error (HTTP 400): BadRequestError: invalid reasoning_content",
        ]

        for raw in final_responses:
            msg = sanitize_feishu_final_response(
                Platform.FEISHU,
                "human",
                raw,
                failed=True,
            )

            assert "这次模型服务没有正常返回" in msg
            assert "HTTP" not in msg
            assert "Upstream request failed" not in msg
            assert "provider" not in msg
            assert "BadRequestError" not in msg

    def test_feishu_human_context_failure_gets_actionable_guidance(self):
        msg = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "⚠️ Session too large for the model's context window.\n"
            "Use /compact to compress the conversation, or /reset to start a fresh session.",
            failed=True,
        )

        assert msg == (
            "这轮对话太长了，模型已经放不下完整上下文。\n"
            "可以先发 /compact 压缩一下，或者 /reset 开一个新会话。"
        )
        assert "Session too large" not in msg
        assert "context window" not in msg

    def test_non_feishu_lifecycle_keeps_raw_diagnostics(self):
        raw = "⏳ Retrying in 2.3s (attempt 1/3)..."

        assert format_feishu_lifecycle_status(
            Platform.TELEGRAM,
            "human",
            "lifecycle",
            raw,
        ) == raw

    def test_feishu_raw_progress_mode_keeps_lifecycle_diagnostics(self):
        raw = "⛔ Provider lane open for 17s (custom:gpt-5.5) — trying fallback..."

        assert format_feishu_lifecycle_status(
            Platform.FEISHU,
            "all",
            "lifecycle",
            raw,
        ) == raw

    def test_feishu_human_final_response_humanizes_python_exception_template(self):
        msg = sanitize_feishu_final_response(
            Platform.FEISHU,
            "human",
            "Sorry, I encountered an error (NameError).\n"
            "name 'session_entry' is not defined\n"
            "Try again or use /reset to start a fresh session.",
            failed=True,
        )

        assert msg == (
            "这次处理没跑完，我这边会按网关日志继续排。\n"
            "你可以稍后重试一次；如果还是这样，我再把具体卡点收出来。"
        )
        assert "NameError" not in msg
        assert "session_entry" not in msg
        assert "Try again" not in msg

    def test_feishu_human_internal_error_hides_credentials_and_provider_hints(self):
        msg = sanitize_feishu_internal_error(
            Platform.FEISHU,
            "human",
            "AuthenticationError",
            "401 invalid API key for provider custom:gpt-5.5",
            status_hint=" Check your API key or run `claude /login` to refresh OAuth credentials.",
        )

        assert "这次处理没跑完" in msg
        assert "API key" not in msg
        assert "custom:gpt-5.5" not in msg
        assert "OAuth" not in msg

    def test_feishu_human_visible_text_humanizes_leaked_tool_and_provider_diagnostics(self):
        examples = [
            "assistant to=functions.browser_snapshot {\"full\": true}\nHTTP 502: provider overloaded",
            "<|channel|>commentary to=functions.mcp_feishu_project_get_workitem_brief {\"work_item_id\":\"123\"}\nBadRequestError: provider rejected request",
            "tool_call: {\"name\": \"terminal\", \"arguments\": {\"command\": \"git status\"}}\nReadError from custom:gpt-5.5",
        ]

        for raw in examples:
            msg = sanitize_feishu_visible_text(Platform.FEISHU, "human", raw)

            assert msg == (
                "这次模型服务没有正常返回，我这边已经自动重试过了。\n"
                "可以稍后再试一次；如果连续出现，我再继续排网关和上游链路。"
            )
            assert "to=functions" not in msg
            assert "tool_call" not in msg
            assert "HTTP" not in msg
            assert "custom:gpt-5.5" not in msg

    def test_feishu_human_visible_text_preserves_normal_prose_with_tool_words(self):
        raw = (
            "这次问题不是工具没跑，而是浏览器自动化这段说明需要再压短一点。\n"
            "我会把 tool call 的展示改成人能看懂的状态，但不会动审计日志。"
        )

        assert sanitize_feishu_visible_text(Platform.FEISHU, "human", raw) == raw

    def test_non_feishu_visible_text_keeps_leaked_diagnostics_for_debugging(self):
        raw = "assistant to=functions.browser_snapshot {}\nHTTP 502: provider overloaded"

        assert sanitize_feishu_visible_text(Platform.TELEGRAM, "human", raw) == raw

    def test_non_feishu_internal_error_keeps_diagnostics(self):
        msg = sanitize_feishu_internal_error(
            Platform.TELEGRAM,
            "human",
            "NameError",
            "name 'session_entry' is not defined",
        )

        assert "Sorry, I encountered an error (NameError)." in msg
        assert "session_entry" in msg
