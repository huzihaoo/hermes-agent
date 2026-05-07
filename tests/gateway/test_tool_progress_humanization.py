"""Regression tests for user-facing gateway tool progress formatting."""

from gateway.config import Platform
from gateway.display_config import resolve_display_setting


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
