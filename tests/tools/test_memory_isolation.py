"""Tests for memory runtime isolation guard."""

import json
import tempfile
from pathlib import Path

import pytest


def test_memory_tool_isolates_users_by_directory():
    """Memory tool should isolate users by directory when user_id is provided."""
    from tools.memory_tool import memory_tool

    with tempfile.TemporaryDirectory() as tmpdir:
        memory_dir = Path(tmpdir)

        # Alice writes to her memory
        result_alice = memory_tool(
            action="add",
            target="user",
            content="Alice's private notes",
            user_id="alice",
            memory_dir=str(memory_dir),
        )
        data_alice = json.loads(result_alice)
        assert data_alice["success"] is True

        # Bob writes to his memory
        result_bob = memory_tool(
            action="add",
            target="user",
            content="Bob's private notes",
            user_id="bob",
            memory_dir=str(memory_dir),
        )
        data_bob = json.loads(result_bob)
        assert data_bob["success"] is True

        # Verify Alice and Bob have separate directories
        alice_dir = memory_dir / "users" / "alice"
        bob_dir = memory_dir / "users" / "bob"
        assert alice_dir.exists()
        assert bob_dir.exists()
        assert (alice_dir / "USER.md").exists()
        assert (bob_dir / "USER.md").exists()

        # Verify content is isolated
        alice_content = (alice_dir / "USER.md").read_text()
        bob_content = (bob_dir / "USER.md").read_text()
        assert "Alice's private notes" in alice_content
        assert "Bob's private notes" not in alice_content
        assert "Bob's private notes" in bob_content
        assert "Alice's private notes" not in bob_content
