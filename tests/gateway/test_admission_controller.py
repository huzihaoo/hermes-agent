"""Tests for the admission controller."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import (
    AdmissionController,
    _classify_domain,
    _classify_lane,
    _resolve_domain_id,
)


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
# Domain classification
# ------------------------------------------------------------------

def test_classify_domain_user_default():
    assert _classify_domain() == "user"
    assert _classify_domain(chat_type="p2p", platform="feishu") == "user"


def test_classify_domain_group():
    assert _classify_domain(chat_type="group", platform="feishu") == "group"


def test_classify_domain_vm():
    assert _classify_domain(platform="vm") == "vm"
    assert _classify_domain(vm_id="vm-123") == "vm"


def test_resolve_domain_id():
    assert _resolve_domain_id("user", "u1", chat_id="c1") == "u1"
    assert _resolve_domain_id("group", "u1", chat_id="c1") == "c1"
    assert _resolve_domain_id("vm", "u1", chat_id="c1", vm_id="vm-1") == "vm-1"


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
        assert "私聊/fast" in feedback
        assert item is not None
        assert item.user_role == "owner"
        assert item.priority == 100
        assert item.domain == "user"
        assert item.domain_id == "u1"


def test_admit_group_message_sets_group_domain():
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )

        import asyncio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            admitted, feedback, item = asyncio.get_event_loop().run_until_complete(
                ctrl.admit(
                    "u1",
                    "帮我查一下这个问题的原因",
                    chat_id="group-chat-1",
                    chat_type="group",
                    platform="feishu",
                )
            )

        assert admitted is True
        assert item.domain == "group"
        assert item.domain_id == "group-chat-1"
        assert "群聊/standard" in feedback


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

        item = ctrl.dequeue_next("fast", domain="user")
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


def test_get_status_shows_queue_state_by_domain():
    """Test that get_status returns current queue state grouped by domain."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )

        import asyncio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            asyncio.get_event_loop().run_until_complete(
                ctrl.admit("user1", "hi", platform="feishu")
            )  # user/fast
            asyncio.get_event_loop().run_until_complete(
                ctrl.admit(
                    "user2",
                    "帮我查一下这个问题的原因",
                    chat_id="group-1",
                    chat_type="group",
                    platform="feishu",
                )
            )  # group/standard
            asyncio.get_event_loop().run_until_complete(
                ctrl.admit(
                    "user3",
                    "帮我写代码实现排序",
                    platform="vm",
                    vm_id="vm-1",
                )
            )  # vm/heavy

        status = ctrl.get_status()

        assert "user" in status
        assert "user" in status
        assert "group" in status
        assert "vm" in status

        # New structure: domain -> domain_id -> lane -> {pending, items}
        assert status["user"]["user1"]["fast"]["pending"] == 1
        assert status["group"]["group-1"]["standard"]["pending"] == 1
        assert status["vm"]["vm-1"]["heavy"]["pending"] == 1

        assert status["user"]["user1"]["fast"]["items"][0]["user_id"] == "user1"
        assert status["group"]["group-1"]["standard"]["items"][0]["user_id"] == "user2"
        assert status["vm"]["vm-1"]["heavy"]["items"][0]["user_id"] == "user3"

        assert "metrics" in status
        assert status["metrics"]["total_admitted"] == 3
        assert status["metrics"]["total_completed"] == 0
