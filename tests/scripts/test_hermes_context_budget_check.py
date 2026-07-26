from __future__ import annotations

from pathlib import Path

from scripts import hermes_context_budget_check as check


REPO_ROOT = Path(__file__).resolve().parents[2]


def _write_marker_repo(root: Path) -> None:
    for relative, markers in check.SOURCE_MARKERS.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(markers), encoding="utf-8")


def test_current_repository_source_layout_passes() -> None:
    ok, missing = check._check_source_markers(REPO_ROOT)

    assert ok is True
    assert missing == []


def test_missing_extracted_guard_fails_closed(tmp_path: Path) -> None:
    _write_marker_repo(tmp_path)
    target = tmp_path / "agent/turn_context.py"
    target.write_text(
        target.read_text(encoding="utf-8").replace(
            "new_tokens < orig_tokens * 0.95", "removed_negative_fixture"
        ),
        encoding="utf-8",
    )

    ok, missing = check._check_source_markers(tmp_path)

    assert ok is False
    assert missing == ["agent/turn_context.py:new_tokens < orig_tokens * 0.95"]


def test_missing_extracted_file_fails_closed(tmp_path: Path) -> None:
    _write_marker_repo(tmp_path)
    (tmp_path / "agent/conversation_loop.py").unlink()

    ok, missing = check._check_source_markers(tmp_path)

    assert ok is False
    assert missing == ["agent/conversation_loop.py:unreadable"]
