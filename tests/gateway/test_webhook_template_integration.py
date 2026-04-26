"""Test webhook routes with template_id integration."""

import time
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

from gateway.tasks.template import TemplateStore


@pytest.fixture
def tmp_template(tmp_path):
    """Create a template and return (template_id, db_path)."""
    db_path = tmp_path / "analytics" / "templates.db"
    store = TemplateStore(db_path=db_path)
    tid = store.create_from_task(
        source_task_id="t1",
        name="PR Review",
        task_type="coding",
        request_summary="Review PR #{pull_request.number}: {pull_request.title}",
        created_at=time.time(),
        skills=["github-code-review", "requesting-code-review"],
    )
    return tid, db_path, tmp_path


class TestWebhookTemplatePrompt:
    """Template prompt used as fallback when route has no explicit prompt."""

    def test_template_prompt_used_when_no_route_prompt(self, tmp_template):
        """If route has template_id but no prompt, use template's request_summary."""
        from gateway.platforms.webhook import WebhookAdapter

        tid, db_path, tmp_path = tmp_template

        adapter = WebhookAdapter.__new__(WebhookAdapter)
        # Minimal init for _render_prompt
        adapter.logger = MagicMock()

        payload = {
            "pull_request": {"number": 42, "title": "Fix login bug"},
        }

        # Simulate what _handle_webhook does:
        route_config = {"template_id": tid}  # no "prompt" key
        prompt_template = route_config.get("prompt", "")

        with patch("hermes_constants.get_hermes_home", return_value=tmp_path):
            # Render prompt from template
            template_id = route_config.get("template_id")
            template_skills = []
            if template_id:
                store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
                tpl = store.get(template_id)
                assert tpl is not None
                if not prompt_template and tpl.get("request_summary"):
                    prompt = adapter._render_prompt(
                        tpl["request_summary"], payload, "pull_request", "test"
                    )
                template_skills = tpl.get("skills", []) or []

        assert "PR #42" in prompt
        assert "Fix login bug" in prompt
        assert template_skills == ["github-code-review", "requesting-code-review"]

    def test_route_prompt_overrides_template(self, tmp_template):
        """If route has both prompt and template_id, route prompt wins."""
        from gateway.platforms.webhook import WebhookAdapter

        tid, db_path, tmp_path = tmp_template

        adapter = WebhookAdapter.__new__(WebhookAdapter)
        adapter.logger = MagicMock()

        payload = {"pull_request": {"number": 42, "title": "Fix login bug"}}

        route_config = {
            "template_id": tid,
            "prompt": "Custom: {pull_request.title}",
        }
        prompt_template = route_config.get("prompt", "")
        prompt = adapter._render_prompt(
            prompt_template, payload, "pull_request", "test"
        )

        # Route prompt should be used, not template
        assert prompt == "Custom: Fix login bug"


class TestWebhookTemplateSkills:
    """Template skills used as fallback when route has no explicit skills."""

    def test_template_skills_used_when_no_route_skills(self, tmp_template):
        """If route has template_id but no skills, use template's skills."""
        tid, db_path, tmp_path = tmp_template

        route_config = {"template_id": tid}  # no "skills" key

        with patch("hermes_constants.get_hermes_home", return_value=tmp_path):
            store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
            tpl = store.get(tid)
            template_skills = tpl.get("skills", []) or []
            skills = route_config.get("skills", []) or template_skills

        assert skills == ["github-code-review", "requesting-code-review"]

    def test_route_skills_override_template(self, tmp_template):
        """If route has both skills and template_id, route skills win."""
        tid, db_path, tmp_path = tmp_template

        route_config = {
            "template_id": tid,
            "skills": ["systematic-debugging"],
        }

        with patch("hermes_constants.get_hermes_home", return_value=tmp_path):
            store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
            tpl = store.get(tid)
            template_skills = tpl.get("skills", []) or []
            skills = route_config.get("skills", []) or template_skills

        assert skills == ["systematic-debugging"]


class TestWebhookTemplateUsageTracking:
    """Template usage is recorded when webhook fires."""

    def test_template_usage_incremented(self, tmp_template):
        """Webhook should increment template usage count."""
        tid, db_path, tmp_path = tmp_template

        store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
        
        # Initial usage should be 0
        tpl = store.get(tid)
        assert tpl["usage_count"] == 0

        # Simulate webhook recording usage
        store.record_usage(tid)
        
        tpl = store.get(tid)
        assert tpl["usage_count"] == 1
