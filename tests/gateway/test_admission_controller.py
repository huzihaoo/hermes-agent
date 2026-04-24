"""Tests for admission controller."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController, _classify_lane


# ------------------------------------------------------------------
# Lane classification
# ------------------------------------------------------------------

def test_classify_lane_short_message():
    assert _classify_lane("你好") == "fast"
    assert _classify_lane("hi") == "fast"


def test_classify_lane_coding_keywords():
    assert _classify_lane("帮我写代码实现一个排序算法") == "heavy"
    assert _classify_lane("please refactor the auth module") == "heavy"


def test_classify_lane_standard():
    assert _classify_lane("帮我分析一下这个文档的内容，总结要点") == "standard"


# ------------------------------------------------------------------
# Admission flow
# ------------------------------------------------------------------

def test_admit_creates_queue_item():
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )

        with patch("gateway.admission.controller._resolve_role", return_value="owner"):
            admitted, feedback, item = ctrl.queue._lock and True, "", None  # dummy
            import asyncio
            admitted, feedback, item = asyncio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello", chat_id="c1")
            )

        assert admitted is True
        assert "fast" in feedback
        assert item is not None
        assert item.user_role == "owner"
        assert item.priority == 100


def test_dequeue_and_complete():
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )

        import asyncio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            asyncio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello", chat_id="c1")
            )

        item = ctrl.dequeue_next("fast")
        assert item is not None

        ctrl.complete(item.id, {"output": "done"})
        completed = ctrl.queue.get_item(item.id)
        assert completed.status == "completed"


def test_audit_files_created():
    with tempfile.TemporaryDirectory() as tmpdir:
        audit_dir = Path(tmpdir) / "audit"
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=audit_dir,
        )

        import asyncio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            asyncio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello")
            )

        log_files = list(audit_dir.glob("*.jsonl"))
        assert len(log_files) >= 1


def test_get_status_shows_queue_state():
    """Test that get_status returns current queue state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )

        import asyncio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            # Enqueue items in different lanes
            asyncio.get_event_loop().run_until_complete(ctrl.admit("user1", "hi", platform="test"))  # fast
            asyncio.get_event_loop().run_until_complete(ctrl.admit("user2", "帮我查一下这个问题的原因", platform="test"))  # standard
            asyncio.get_event_loop().run_until_complete(ctrl.admit("user3", "帮我写代码实现排序", platform="test"))  # heavy

        status = ctrl.get_status()

        # Verify structure
        assert "fast" in status
        assert "standard" in status
        assert "heavy" in status

        # Verify counts
        assert status["fast"]["pending"] == 1
        assert status["standard"]["pending"] == 1
        assert status["heavy"]["pending"] == 1

        # Verify item details
        assert len(status["fast"]["items"]) == 1
        assert status["fast"]["items"][0]["user_id"] == "user1"
        assert "message_preview" in status["fast"]["items"][0]
        
        # Verify metrics
        assert "metrics" in status
        assert status["metrics"]["total_admitted"] == 3
        assert status["metrics"]["total_completed"] == 0
