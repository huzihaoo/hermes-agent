"""Tests for skill_view content optimization — frontmatter stripping and truncation.

Ref: knowledge/wiki/systems/session-memory-protection.md
Goal: Reduce context window pressure from large skill_view returns.
"""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from tools.skills_tool import skill_view, SKILLS_DIR


def _make_skill(base_dir: Path, name: str, body: str, frontmatter_extra: str = ""):
    """Create a skill directory with SKILL.md."""
    skill_dir = base_dir / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    content = (
        "---\n"
        f"name: {name}\n"
        f"description: Test skill {name}\n"
        f"{frontmatter_extra}"
        "---\n"
        f"{body}"
    )
    (skill_dir / "SKILL.md").write_text(content, encoding="utf-8")
    return skill_dir


@pytest.fixture(autouse=True)
def isolate_skills(tmp_path, monkeypatch):
    """Redirect SKILLS_DIR to a temp directory."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    monkeypatch.setattr("tools.skills_tool.SKILLS_DIR", skills_dir)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return skills_dir


class TestFrontmatterStripping:
    """skill_view should strip frontmatter from the returned content field."""

    def test_content_does_not_start_with_frontmatter(self, isolate_skills):
        _make_skill(isolate_skills, "clean-skill", "# Hello\n\nBody text here.")
        result = json.loads(skill_view("clean-skill"))
        assert result["success"] is True
        # Content should NOT start with '---' (frontmatter delimiter)
        assert not result["content"].startswith("---")
        # But the body should be present
        assert "# Hello" in result["content"]
        assert "Body text here." in result["content"]

    def test_frontmatter_fields_still_populated(self, isolate_skills):
        _make_skill(
            isolate_skills,
            "meta-skill",
            "# Content\n\nSome body.",
            frontmatter_extra="metadata:\n  hermes:\n    tags: [test, demo]\n    related_skills: [other]\n",
        )
        result = json.loads(skill_view("meta-skill"))
        assert result["success"] is True
        assert result["description"] == "Test skill meta-skill"
        assert "test" in result["tags"]
        # Content should be body only
        assert not result["content"].startswith("---")
        assert "# Content" in result["content"]

    def test_content_without_frontmatter_unchanged(self, isolate_skills):
        """If content has no frontmatter, it should pass through unchanged."""
        skill_dir = isolate_skills / "bare-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("# Just a heading\n\nNo frontmatter.", encoding="utf-8")
        result = json.loads(skill_view("bare-skill"))
        assert result["success"] is True
        assert result["content"] == "# Just a heading\n\nNo frontmatter."


class TestContentTruncation:
    """skill_view should truncate very large content with a note."""

    def test_small_content_not_truncated(self, isolate_skills):
        body = "# Small\n\n" + ("x" * 1000)
        _make_skill(isolate_skills, "small-skill", body)
        result = json.loads(skill_view("small-skill"))
        assert result["success"] is True
        assert "x" * 1000 in result["content"]
        assert result.get("truncated") is not True

    def test_large_content_truncated(self, isolate_skills):
        from tools.skills_tool import SKILL_VIEW_MAX_CONTENT_CHARS
        body = "# Large\n\n" + ("x" * (SKILL_VIEW_MAX_CONTENT_CHARS + 5000))
        _make_skill(isolate_skills, "large-skill", body)
        result = json.loads(skill_view("large-skill"))
        assert result["success"] is True
        # Content should be truncated
        assert len(result["content"]) <= SKILL_VIEW_MAX_CONTENT_CHARS + 500  # allow for truncation note
        assert result["truncated"] is True
        # Should contain a truncation notice
        assert "truncated" in result["content"].lower() or "truncated" in str(result.get("truncation_note", "")).lower()

    def test_truncation_preserves_beginning(self, isolate_skills):
        from tools.skills_tool import SKILL_VIEW_MAX_CONTENT_CHARS
        body = "# Important Header\n\nCritical first paragraph.\n\n" + ("x" * (SKILL_VIEW_MAX_CONTENT_CHARS + 5000))
        _make_skill(isolate_skills, "trunc-skill", body)
        result = json.loads(skill_view("trunc-skill"))
        assert result["success"] is True
        assert "# Important Header" in result["content"]
        assert "Critical first paragraph." in result["content"]

    def test_file_path_returns_full_content(self, isolate_skills):
        """file_path access should NOT truncate — it's an explicit request."""
        from tools.skills_tool import SKILL_VIEW_MAX_CONTENT_CHARS
        body = "# Main\n\nShort."
        skill_dir = _make_skill(isolate_skills, "ref-skill", body)
        refs_dir = skill_dir / "references"
        refs_dir.mkdir()
        large_ref = "# Reference\n\n" + ("y" * (SKILL_VIEW_MAX_CONTENT_CHARS + 5000))
        (refs_dir / "big.md").write_text(large_ref, encoding="utf-8")
        result = json.loads(skill_view("ref-skill", file_path="references/big.md"))
        assert result["success"] is True
        # file_path access should return full content
        assert len(result["content"]) > SKILL_VIEW_MAX_CONTENT_CHARS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
