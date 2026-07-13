"""Tests for persistence and recovery of the admission controller."""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.queue import AdmissionPersistenceError


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


@pytest.mark.asyncio
async def test_durable_enqueue_failure_rolls_back_memory_and_fails_closed(tmp_path):
    controller = _make_controller(tmp_path)
    from gateway.admission.persistence import init_db

    init_db(tmp_path / "queue.db")

    with (
        patch("gateway.admission.controller._resolve_role", return_value="member"),
        patch(
            "gateway.admission.persistence.save_items",
            side_effect=sqlite3.OperationalError("disk unavailable"),
        ) as save_items,
    ):
        with pytest.raises(AdmissionPersistenceError, match="durable enqueue failed"):
            await controller.admit(
                "ou_user",
                "分析问题",
                chat_id="oc_group",
                chat_type="group",
                request_message_id="om_durable_failure",
                platform="feishu",
                event_context={"schema_version": "test_v1"},
                require_durable_persistence=True,
            )

        assert controller.queue.pending_count() == 0
        assert controller.get_status()["metrics"]["total_admitted"] == 0
        assert controller._user_timestamps["ou_user"] == []
        assert controller.queue.persistence_healthy is False

        with pytest.raises(AdmissionPersistenceError, match="persistence is unhealthy"):
            await controller.admit(
                "ou_user",
                "再次投递",
                chat_id="oc_group",
                chat_type="group",
                request_message_id="om_durable_failure",
                platform="feishu",
                event_context={"schema_version": "test_v1"},
                require_durable_persistence=True,
            )

    assert save_items.call_count == 1
    with sqlite3.connect(tmp_path / "queue.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM queue_items").fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("column", "corrupt_value"),
    [
        ("result", "{not-json"),
        ("event_context", json.dumps(["not", "an", "object"])),
    ],
)
async def test_corrupt_row_latches_unhealthy_without_deleting_rows(
    tmp_path,
    column,
    corrupt_value,
):
    first = _make_controller(tmp_path)
    with patch("gateway.admission.controller._resolve_role", return_value="member"):
        _, _, corrupt_item = await first.admit(
            "u1",
            "first",
            request_message_id="om_corrupt",
            platform="feishu",
        )
        _, _, good_item = await first.admit(
            "u2",
            "second",
            request_message_id="om_good",
            platform="feishu",
        )

    with sqlite3.connect(tmp_path / "queue.db") as conn:
        conn.execute(
            f"UPDATE queue_items SET {column} = ? WHERE id = ?",
            (corrupt_value, corrupt_item.id),
        )
        conn.commit()
        before = conn.execute(
            "SELECT id, result, event_context FROM queue_items ORDER BY id"
        ).fetchall()

    recovered = _make_controller(tmp_path)
    assert recovered.queue.get_item(corrupt_item.id) is None
    assert recovered.queue.get_item(good_item.id) is not None
    assert recovered.queue.persistence_healthy is False
    assert recovered.get_status()["persistence"]["errors"]

    with pytest.raises(AdmissionPersistenceError, match="persistence is unhealthy"):
        await recovered.admit(
            "u3",
            "must not save a partial snapshot",
            request_message_id="om_new",
            platform="feishu",
            require_durable_persistence=True,
        )
    with pytest.raises(AdmissionPersistenceError, match="persistence is unhealthy"):
        recovered.dequeue_next(good_item.lane, domain=good_item.domain)
    with pytest.raises(AdmissionPersistenceError, match="persistence is unhealthy"):
        recovered.queue.cleanup_old_items()

    with sqlite3.connect(tmp_path / "queue.db") as conn:
        after = conn.execute(
            "SELECT id, result, event_context FROM queue_items ORDER BY id"
        ).fetchall()
    assert after == before


@pytest.mark.asyncio
async def test_metrics_failure_does_not_undo_durable_queue_commit(tmp_path):
    controller = _make_controller(tmp_path)
    with (
        patch("gateway.admission.controller._resolve_role", return_value="member"),
        patch(
            "gateway.admission.persistence.save_metrics",
            side_effect=sqlite3.OperationalError("metrics unavailable"),
        ),
    ):
        admitted, _, item = await controller.admit(
            "ou_user",
            "分析问题",
            request_message_id="om_metrics_failure",
            platform="feishu",
            event_context={"schema_version": "test_v1"},
            require_durable_persistence=True,
        )

    assert admitted is True
    assert item is not None
    assert controller.queue.persistence_healthy is True
    restarted = _make_controller(tmp_path)
    assert restarted.queue.get_item(item.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["complete", "retry"])
async def test_durable_state_transition_failure_restores_processing_owner(
    tmp_path,
    transition,
):
    controller = _make_controller(tmp_path)
    with patch("gateway.admission.controller._resolve_role", return_value="member"):
        _, _, item = await controller.admit(
            "ou_user",
            "分析问题",
            request_message_id=f"om_{transition}_failure",
            platform="feishu",
            require_durable_persistence=True,
        )
    assert controller.dequeue_next(item.lane, domain=item.domain) is item

    with patch(
        "gateway.admission.persistence.save_items",
        side_effect=sqlite3.OperationalError("disk unavailable"),
    ):
        with pytest.raises(
            AdmissionPersistenceError,
            match="durable state transition failed",
        ):
            if transition == "complete":
                controller.complete(item.id, {"durable_feishu_completion": True})
            else:
                controller.fail(item.id, "retry me")

    assert item.status == "processing"
    assert item.retry_count == 0
    assert item.result is None
    assert controller.queue.persistence_healthy is False
    with sqlite3.connect(tmp_path / "queue.db") as conn:
        persisted = conn.execute(
            "SELECT status, retry_count, result FROM queue_items WHERE id = ?",
            (item.id,),
        ).fetchone()
    assert persisted == ("processing", 0, None)


def test_queue_survives_restart():
    """Items in queue should persist across controller restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # First session: admit 3 items
        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )
            _, _, item3 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg3", chat_id="c1")
            )

        # Restart controller
        ctrl2 = _make_controller(tmp_path)

        # All items should still be there
        stored1 = ctrl2.queue.get_item(item1.id)
        stored2 = ctrl2.queue.get_item(item2.id)
        stored3 = ctrl2.queue.get_item(item3.id)

        assert stored1 is not None
        assert stored2 is not None
        assert stored3 is not None

        assert stored1.status == "queued"
        assert stored2.status == "queued"
        assert stored3.status == "queued"

        # Pending counts should match
        status = ctrl2.get_status()
        # ctrl1 admitted 3 items, ctrl2 loaded metrics from disk
        assert status["metrics"]["total_admitted"] >= 3


def test_processing_items_are_requeued_after_restart():
    """No live worker owns a persisted processing item after restart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        # Dequeue to mark as processing
        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        assert dequeued.status == "processing"

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "queued"
        assert stored.started_at is None
        assert ctrl2.queue.pending_count() == 1
        recovered = ctrl2.dequeue_next(stored.lane, domain=stored.domain)
        assert recovered is not None
        assert recovered.id == item.id


def test_feishu_event_context_and_idempotency_survive_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        context = {
            "schema_version": "feishu_queue_message_event_v1",
            "route_contract": "g1q3_rca_manual_v1",
            "source": {"user_id": "ou_user"},
        }
        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio

        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, first = _aio.get_event_loop().run_until_complete(
                ctrl1.admit(
                    "ou_user",
                    "分析问题",
                    chat_id="oc_group",
                    chat_type="group",
                    request_message_id="om_request",
                    platform="feishu",
                    event_context=context,
                )
            )
            _, _, duplicate = _aio.get_event_loop().run_until_complete(
                ctrl1.admit(
                    "ou_user",
                    "分析问题",
                    chat_id="oc_group",
                    chat_type="group",
                    request_message_id="om_request",
                    platform="feishu",
                    event_context=context,
                )
            )

        assert duplicate is first
        assert ctrl1.queue.pending_count() == 1

        ctrl2 = _make_controller(tmp_path)
        restored = ctrl2.queue.get_item(first.id)
        assert restored is not None
        assert restored.event_context == context
        assert ctrl2.queue.pending_count() == 1


def test_completed_items_survive_restart():
    """Completed items should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.complete(item.id, {"result": "ok"})

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "completed"
        assert stored.result == {"result": "ok"}


def test_dead_items_survive_restart():
    """Dead-lettered items should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        item.max_retries = 0
        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.fail(item.id, "instant death")

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "dead"
        assert stored.last_error == "instant death"


def test_audit_log_persists():
    """Audit log should be written to disk and survive restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1", platform="feishu")
            )

        # Audit file should exist
        audit_dir = tmp_path / "audit"
        audit_files = list(audit_dir.glob("*.jsonl"))
        assert len(audit_files) > 0

        # Read audit log
        import json
        with open(audit_files[0]) as f:
            lines = f.readlines()
            assert len(lines) >= 1
            entry = json.loads(lines[0])
            assert entry["action"] == "enqueue"
            assert entry["resource"] == item.id
            assert entry["user_id"] == "u1"


def test_metrics_survive_restart():
    """Metrics should be recalculated correctly after restart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )

        # Complete one, fail one
        dequeued1 = ctrl1.dequeue_next(item1.lane, domain=item1.domain)
        ctrl1.complete(item1.id, {"ok": True})

        dequeued2 = ctrl1.dequeue_next(item2.lane, domain=item2.domain)
        item2.max_retries = 0
        ctrl1.fail(item2.id, "err")

        # Restart
        ctrl2 = _make_controller(tmp_path)

        status = ctrl2.get_status()
        assert status["metrics"]["total_completed"] >= 1
        assert status["metrics"]["total_dead"] >= 1


def test_lane_state_survives_restart():
    """Lane-specific state should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )

        # Restart
        ctrl2 = _make_controller(tmp_path)

        # Each domain_id should have 1 item
        assert ctrl2.queue.pending_count(lane=item1.lane, domain=item1.domain, domain_id=item1.domain_id) == 1
        assert ctrl2.queue.pending_count(lane=item2.lane, domain=item2.domain, domain_id=item2.domain_id) == 1


def test_retry_state_survives_restart():
    """Retry count and next_retry_at should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.fail(item.id, "err")

        stored1 = ctrl1.queue.get_item(item.id)
        assert stored1.retry_count == 1
        assert stored1.next_retry_at is not None

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored2 = ctrl2.queue.get_item(item.id)
        assert stored2.retry_count == 1
        assert stored2.next_retry_at == stored1.next_retry_at
