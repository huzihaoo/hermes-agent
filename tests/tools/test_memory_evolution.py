"""Test auto memory review and error evolution.

Tests the correction detection and auto-save logic without importing
the full AIAgent (which has heavy dependencies).
"""
import json
import os
from pathlib import Path

import pytest


# Correction patterns — same as AIAgent._CORRECTION_PATTERNS
_CORRECTION_PATTERNS = (
    "不对", "不是这样", "错了", "你搞错了", "不要这样",
    "remember this", "记住", "别再", "don't do that",
    "wrong", "incorrect", "no, ", "nope",
)


@pytest.fixture(autouse=True)
def _isolate_hermes_home(tmp_path, monkeypatch):
    """Ensure HERMES_HOME is scoped to each test and cleaned up."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))


def _detect_corrections(messages: list) -> list[str]:
    corrections = []
    for msg in messages:
        if msg.get("role") != "user":
            continue
        text = (msg.get("content") or "").lower()
        if any(p in text for p in _CORRECTION_PATTERNS):
            corrections.append(msg.get("content", ""))
    return corrections


def _auto_memory_review(store, messages: list) -> None:
    if not store:
        return
    corrections = _detect_corrections(messages)
    if not corrections:
        return
    from tools.memory_tool import memory_tool
    for correction_text in corrections[:3]:
        snippet = correction_text.strip()[:200]
        if not snippet:
            continue
        memory_tool(action="add", target="memory",
                    content=f"User correction: {snippet}", store=store)


def test_detect_corrections_chinese():
    messages = [
        {"role": "user", "content": "帮我查一下天气"},
        {"role": "assistant", "content": "今天晴天"},
        {"role": "user", "content": "不对，我要查明天的"},
        {"role": "assistant", "content": "明天多云"},
        {"role": "user", "content": "你搞错了，我说的是上海"},
    ]
    assert len(_detect_corrections(messages)) == 2


def test_detect_corrections_english():
    messages = [
        {"role": "user", "content": "What's 2+2?"},
        {"role": "assistant", "content": "5"},
        {"role": "user", "content": "No, that's wrong. It's 4."},
        {"role": "user", "content": "Remember this for next time"},
    ]
    assert len(_detect_corrections(messages)) == 2


def test_detect_no_corrections():
    messages = [
        {"role": "user", "content": "帮我写个函数"},
        {"role": "assistant", "content": "def hello(): pass"},
        {"role": "user", "content": "谢谢，很好"},
    ]
    assert len(_detect_corrections(messages)) == 0


def test_auto_memory_review_saves_corrections():
    from tools.memory_tool import MemoryStore
    store = MemoryStore(user_id="test_user")
    store.load_from_disk()
    messages = [
        {"role": "user", "content": "查一下北京天气"},
        {"role": "assistant", "content": "上海今天晴"},
        {"role": "user", "content": "错了，我说的是北京不是上海"},
    ]
    _auto_memory_review(store, messages)
    store_check = MemoryStore(user_id="test_user")
    store_check.load_from_disk()
    assert len(store_check.memory_entries) > 0
    assert "User correction:" in store_check.memory_entries[0]
    assert "北京" in store_check.memory_entries[0]


def test_auto_memory_review_caps_at_3():
    from tools.memory_tool import MemoryStore
    store = MemoryStore(user_id="test_cap")
    store.load_from_disk()
    messages = [{"role": "user", "content": f"不对 correction {i}"} for i in range(5)]
    _auto_memory_review(store, messages)
    store_check = MemoryStore(user_id="test_cap")
    store_check.load_from_disk()
    assert len(store_check.memory_entries) == 3


def test_auto_memory_review_no_crash_without_store():
    _auto_memory_review(None, [{"role": "user", "content": "错了"}])


def test_corrections_persist_across_sessions():
    from tools.memory_tool import MemoryStore
    store1 = MemoryStore(user_id="persist_user")
    store1.load_from_disk()
    _auto_memory_review(store1, [
        {"role": "user", "content": "错了，默认用 UTC+8 时区"},
    ])
    store2 = MemoryStore(user_id="persist_user")
    store2.load_from_disk()
    assert any("UTC+8" in e for e in store2.memory_entries)


def test_corrections_isolated_between_users():
    from tools.memory_tool import MemoryStore
    store_a = MemoryStore(user_id="alice")
    store_a.load_from_disk()
    _auto_memory_review(store_a, [
        {"role": "user", "content": "不对，我的名字是 Alice"},
    ])
    store_b = MemoryStore(user_id="bob")
    store_b.load_from_disk()
    assert len(store_b.memory_entries) == 0
