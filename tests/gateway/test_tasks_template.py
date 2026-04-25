"""Tests for gateway.tasks.template — TemplateStore."""

import time
import pytest
from gateway.tasks.template import TemplateStore


@pytest.fixture
def store(tmp_path):
    return TemplateStore(db_path=tmp_path / "templates.db")


def test_create_and_get(store):
    tid = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写日报",
        created_at=time.time(),
    )
    assert tid  # non-empty UUID
    tpl = store.get(tid)
    assert tpl is not None
    assert tpl["source_task_id"] == "t1"
    assert tpl["name"] == "日报模板"
    assert tpl["task_type"] == "docs"
    assert tpl["request_summary"] == "写日报"


def test_get_nonexistent(store):
    assert store.get("nope") is None


def test_list_recent_returns_sorted(store):
    now = time.time()
    ids = []
    for i, ts in enumerate([1000.0, 3000.0, 2000.0]):
        tid = store.create_from_task(
            source_task_id=f"t{i}",
            name=f"tpl-{i}",
            task_type="chat",
            request_summary=f"task {i}",
            created_at=ts,
        )
        ids.append(tid)
    templates = store.list_recent(limit=10)
    assert len(templates) == 3
    assert templates[0]["source_task_id"] == "t1"  # ts=3000 is most recent
    assert templates[1]["source_task_id"] == "t2"  # ts=2000
    assert templates[2]["source_task_id"] == "t0"  # ts=1000


def test_list_recent_respects_limit(store):
    now = time.time()
    for i in range(15):
        store.create_from_task(
            source_task_id=f"t{i}",
            name=f"tpl-{i}",
            task_type="chat",
            request_summary=f"task {i}",
            created_at=now + i,
        )
    templates = store.list_recent(limit=5)
    assert len(templates) == 5
