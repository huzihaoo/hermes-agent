"""Tests for feishu_comment — event filtering, access control integration, wiki reverse lookup."""

import asyncio
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from plugins.platforms.feishu.feishu_comment import (
    parse_drive_comment_event,
    _ALLOWED_NOTICE_TYPES,
    _sanitize_comment_text,
)


def _make_event(
    comment_id="c1",
    reply_id="r1",
    notice_type="add_reply",
    file_token="docx_token",
    file_type="docx",
    from_open_id="ou_user",
    to_open_id="ou_bot",
    is_mentioned=True,
):
    """Build a minimal drive comment event SimpleNamespace."""
    return SimpleNamespace(event={
        "event_id": "evt_1",
        "comment_id": comment_id,
        "reply_id": reply_id,
        "is_mentioned": is_mentioned,
        "timestamp": "1713200000",
        "notice_meta": {
            "file_token": file_token,
            "file_type": file_type,
            "notice_type": notice_type,
            "from_user_id": {"open_id": from_open_id},
            "to_user_id": {"open_id": to_open_id},
        },
    })


class TestParseEvent(unittest.TestCase):
    def test_parse_valid_event(self):
        evt = _make_event()
        parsed = parse_drive_comment_event(evt)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["comment_id"], "c1")
        self.assertEqual(parsed["file_type"], "docx")
        self.assertEqual(parsed["from_open_id"], "ou_user")
        self.assertEqual(parsed["to_open_id"], "ou_bot")

    def test_parse_missing_event_attr(self):
        self.assertIsNone(parse_drive_comment_event(object()))

    def test_parse_none_event(self):
        self.assertIsNone(parse_drive_comment_event(SimpleNamespace()))


class TestEventFiltering(unittest.TestCase):
    """Test the filtering logic in handle_drive_comment_event."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed")
    def test_self_reply_filtered(self, mock_allowed, mock_resolve, mock_load):
        """Events where from_open_id == self_open_id should be dropped."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event

        evt = _make_event(from_open_id="ou_bot", to_open_id="ou_bot")
        self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        mock_load.assert_not_called()

    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed")
    def test_wrong_receiver_filtered(self, mock_allowed, mock_resolve, mock_load):
        """Events where to_open_id != self_open_id should be dropped."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event

        evt = _make_event(to_open_id="ou_other_bot")
        self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        mock_load.assert_not_called()

    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed")
    def test_empty_to_open_id_filtered(self, mock_allowed, mock_resolve, mock_load):
        """Events with empty to_open_id should be dropped."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event

        evt = _make_event(to_open_id="")
        self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        mock_load.assert_not_called()

    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed")
    def test_invalid_notice_type_filtered(self, mock_allowed, mock_resolve, mock_load):
        """Events with unsupported notice_type should be dropped."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event

        evt = _make_event(notice_type="resolve_comment")
        self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        mock_load.assert_not_called()

    def test_allowed_notice_types(self):
        self.assertIn("add_comment", _ALLOWED_NOTICE_TYPES)
        self.assertIn("add_reply", _ALLOWED_NOTICE_TYPES)
        self.assertNotIn("resolve_comment", _ALLOWED_NOTICE_TYPES)


class TestAccessControlIntegration(unittest.TestCase):
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    @patch("plugins.platforms.feishu.feishu_comment_rules.has_wiki_keys", return_value=False)
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed", return_value=False)
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    def test_denied_user_no_side_effects(self, mock_load, mock_resolve, mock_allowed, mock_wiki_keys):
        """Denied user should not trigger typing reaction or agent."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event
        from plugins.platforms.feishu.feishu_comment_rules import ResolvedCommentRule

        mock_resolve.return_value = ResolvedCommentRule(True, "allowlist", frozenset(), "top")
        mock_load.return_value = Mock()

        client = Mock()
        evt = _make_event()
        self._run(handle_drive_comment_event(client, evt, self_open_id="ou_bot"))

        # No API calls should be made for denied users
        client.request.assert_not_called()

    @patch("plugins.platforms.feishu.feishu_comment_rules.has_wiki_keys", return_value=False)
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed", return_value=False)
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    def test_disabled_comment_skipped(self, mock_load, mock_resolve, mock_allowed, mock_wiki_keys):
        """Disabled comments should return immediately."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event
        from plugins.platforms.feishu.feishu_comment_rules import ResolvedCommentRule

        mock_resolve.return_value = ResolvedCommentRule(False, "allowlist", frozenset(), "top")
        mock_load.return_value = Mock()

        evt = _make_event()
        self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        mock_allowed.assert_not_called()


class TestSanitizeCommentText(unittest.TestCase):
    def test_angle_brackets_escaped(self):
        self.assertEqual(_sanitize_comment_text("List<String>"), "List&lt;String&gt;")

    def test_ampersand_escaped_first(self):
        self.assertEqual(_sanitize_comment_text("a & b"), "a &amp; b")

    def test_ampersand_not_double_escaped(self):
        result = _sanitize_comment_text("a < b & c > d")
        self.assertEqual(result, "a &lt; b &amp; c &gt; d")
        self.assertNotIn("&amp;lt;", result)
        self.assertNotIn("&amp;gt;", result)

    def test_plain_text_unchanged(self):
        self.assertEqual(_sanitize_comment_text("hello world"), "hello world")

    def test_empty_string(self):
        self.assertEqual(_sanitize_comment_text(""), "")

    def test_code_snippet(self):
        text = 'if (a < b && c > 0) { return "ok"; }'
        result = _sanitize_comment_text(text)
        self.assertNotIn("<", result)
        self.assertNotIn(">", result)
        self.assertIn("&lt;", result)
        self.assertIn("&gt;", result)


class TestCommentAgentTools(unittest.TestCase):
    def test_aily_is_default_off_and_requires_direct_feishu_opt_in(self):
        from plugins.platforms.feishu.feishu_comment import _comment_agent_toolsets

        self.assertEqual(_comment_agent_toolsets({}), ["feishu_doc", "feishu_drive"])
        self.assertEqual(
            _comment_agent_toolsets(
                {"platform_toolsets": {"feishu": ["hermes-feishu"]}}
            ),
            ["feishu_doc", "feishu_drive"],
        )
        self.assertEqual(
            _comment_agent_toolsets(
                {"platform_toolsets": {"feishu": ["feishu_aily"]}}
            ),
            ["feishu_doc", "feishu_drive", "feishu_aily"],
        )
        self.assertEqual(
            _comment_agent_toolsets(
                {"platform_toolsets": {"feishu": ["feishu_aily_agent"]}}
            ),
            ["feishu_doc", "feishu_drive", "feishu_aily_agent"],
        )
        self.assertEqual(
            _comment_agent_toolsets(
                {
                    "platform_toolsets": {"feishu": ["feishu_aily"]},
                    "agent": {"disabled_toolsets": ["feishu_aily"]},
                }
            ),
            ["feishu_doc", "feishu_drive"],
        )

    def test_explicit_aily_client_is_injected_and_restored(self):
        from plugins.platforms.feishu import feishu_comment
        from tools.feishu_aily_knowledge_tool import (
            get_client as get_aily_client,
            reset_client as reset_aily_client,
            set_client as set_aily_client,
        )
        from tools.feishu_doc_tool import get_client as get_doc_client
        from tools.feishu_drive_tool import get_client as get_drive_client

        client = Mock()
        previous_client = Mock()
        observed = {}
        outer_token = set_aily_client(previous_client)

        class FakeAgent:
            def __init__(self, **kwargs):
                observed["toolsets"] = kwargs["enabled_toolsets"]
                observed["clients"] = (
                    get_doc_client(),
                    get_drive_client(),
                    get_aily_client(),
                )

            def run_conversation(self, prompt, conversation_history=None):
                return {"final_response": "ok", "api_calls": 0, "messages": []}

        fake_run_agent = SimpleNamespace(AIAgent=FakeAgent)
        runtime = {
            "provider": "test",
            "base_url": None,
            "api_key": None,
            "api_mode": None,
            "credential_pool": None,
        }
        try:
            with patch.dict(sys.modules, {"run_agent": fake_run_agent}), patch.object(
                feishu_comment,
                "_resolve_model_and_runtime",
                return_value=("test-model", runtime),
            ), patch.object(
                feishu_comment,
                "_comment_agent_toolsets",
                return_value=["feishu_doc", "feishu_drive", "feishu_aily"],
            ):
                result = feishu_comment._run_comment_agent("question", client)

            self.assertEqual(result, "ok")
            self.assertEqual(
                observed["toolsets"], ["feishu_doc", "feishu_drive", "feishu_aily"]
            )
            self.assertEqual(observed["clients"], (client, client, client))
            self.assertIsNone(get_doc_client())
            self.assertIsNone(get_drive_client())
            self.assertIs(get_aily_client(), previous_client)
        finally:
            reset_aily_client(outer_token)

    def test_executor_propagates_profile_secret_scope(self):
        from agent import secret_scope
        from plugins.platforms.feishu import feishu_comment

        previous_mode = secret_scope.is_multiplex_active()
        secret_scope.set_multiplex_active(True)
        token = secret_scope.set_secret_scope({"COMMENT_PROFILE_MARKER": "profile-a"})

        def worker(_prompt, _client, _session_key):
            return secret_scope.get_secret("COMMENT_PROFILE_MARKER")

        try:
            with patch.object(feishu_comment, "_run_comment_agent", worker):
                result = asyncio.run(
                    feishu_comment._run_comment_agent_in_executor("q", Mock(), "session")
                )
            self.assertEqual(result, "profile-a")
        finally:
            secret_scope.reset_secret_scope(token)
            secret_scope.set_multiplex_active(previous_mode)


class TestWikiReverseLookup(unittest.TestCase):
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    @patch("plugins.platforms.feishu.feishu_comment._exec_request")
    def test_reverse_lookup_success(self, mock_exec):
        from plugins.platforms.feishu.feishu_comment import _reverse_lookup_wiki_token

        mock_exec.return_value = (0, "Success", {
            "node": {"node_token": "WIKI_TOKEN_123", "obj_token": "docx_abc"},
        })
        result = self._run(_reverse_lookup_wiki_token(Mock(), "docx", "docx_abc"))
        self.assertEqual(result, "WIKI_TOKEN_123")
        # Verify correct API params
        call_args = mock_exec.call_args
        queries = call_args[1].get("queries") or call_args[0][3]
        query_dict = dict(queries)
        self.assertEqual(query_dict["token"], "docx_abc")
        self.assertEqual(query_dict["obj_type"], "docx")

    @patch("plugins.platforms.feishu.feishu_comment._exec_request")
    def test_reverse_lookup_not_wiki(self, mock_exec):
        from plugins.platforms.feishu.feishu_comment import _reverse_lookup_wiki_token

        mock_exec.return_value = (131001, "not found", {})
        result = self._run(_reverse_lookup_wiki_token(Mock(), "docx", "docx_abc"))
        self.assertIsNone(result)

    @patch("plugins.platforms.feishu.feishu_comment._exec_request")
    def test_reverse_lookup_service_error(self, mock_exec):
        from plugins.platforms.feishu.feishu_comment import _reverse_lookup_wiki_token

        mock_exec.return_value = (500, "internal error", {})
        result = self._run(_reverse_lookup_wiki_token(Mock(), "docx", "docx_abc"))
        self.assertIsNone(result)

    @patch("plugins.platforms.feishu.feishu_comment._reverse_lookup_wiki_token", new_callable=AsyncMock)
    @patch("plugins.platforms.feishu.feishu_comment_rules.has_wiki_keys", return_value=True)
    @patch("plugins.platforms.feishu.feishu_comment_rules.is_user_allowed", return_value=True)
    @patch("plugins.platforms.feishu.feishu_comment_rules.resolve_rule")
    @patch("plugins.platforms.feishu.feishu_comment_rules.load_config")
    @patch("plugins.platforms.feishu.feishu_comment.add_comment_reaction", new_callable=AsyncMock)
    @patch("plugins.platforms.feishu.feishu_comment.batch_query_comment", new_callable=AsyncMock)
    @patch("plugins.platforms.feishu.feishu_comment.query_document_meta", new_callable=AsyncMock)
    def test_wiki_lookup_triggered_when_no_exact_match(
        self, mock_meta, mock_batch, mock_reaction,
        mock_load, mock_resolve, mock_allowed, mock_wiki_keys, mock_lookup,
    ):
        """Wiki reverse lookup should fire when rule falls to wildcard/top and wiki keys exist."""
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event
        from plugins.platforms.feishu.feishu_comment_rules import ResolvedCommentRule

        # First resolve returns wildcard (no exact match), second returns exact wiki match
        mock_resolve.side_effect = [
            ResolvedCommentRule(True, "allowlist", frozenset(), "wildcard"),
            ResolvedCommentRule(True, "allowlist", frozenset(), "exact:wiki:WIKI123"),
        ]
        mock_load.return_value = Mock()
        mock_lookup.return_value = "WIKI123"
        mock_meta.return_value = {"title": "Test", "url": ""}
        mock_batch.return_value = {"is_whole": False, "quote": ""}

        evt = _make_event()
        # Will proceed past access control but fail later — that's OK, we just test the lookup
        try:
            self._run(handle_drive_comment_event(Mock(), evt, self_open_id="ou_bot"))
        except Exception:
            pass

        mock_lookup.assert_called_once_with(unittest.mock.ANY, "docx", "docx_token")
        self.assertEqual(mock_resolve.call_count, 2)
        # Second call should include wiki_token
        second_call_kwargs = mock_resolve.call_args_list[1]
        self.assertEqual(second_call_kwargs[1].get("wiki_token") or second_call_kwargs[0][3], "WIKI123")


if __name__ == "__main__":
    unittest.main()
