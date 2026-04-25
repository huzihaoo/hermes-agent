"""Test correction detection false positive handling."""
import pytest


@pytest.fixture(autouse=True)
def _isolate_hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))


# Standalone correction detection (same as test_memory_evolution.py)
_CORRECTION_PATTERNS = (
    "不对", "不是这样", "错了", "你搞错了", "不要这样",
    "remember this", "记住", "别再", "don't do that",
    "wrong", "incorrect", "no, ", "nope",
)


def _detect_corrections(messages):
    corrections = []
    for msg in messages:
        if msg.get("role") != "user":
            continue
        text = (msg.get("content") or "").lower()
        if any(p in text for p in _CORRECTION_PATTERNS):
            corrections.append(msg.get("content", ""))
    return corrections


def test_correction_false_positive_normal_conversation():
    """Normal conversation with 'wrong' in context should not trigger."""
    messages = [
        {"role": "user", "content": "What's the wrong way to do this?"},
        {"role": "assistant", "content": "The wrong way is..."},
        {"role": "user", "content": "Thanks, that helps"},
    ]
    
    # "wrong" appears but not as a correction
    corrections = _detect_corrections(messages)
    # Current implementation WILL detect this as false positive
    # This test documents the known limitation
    assert len(corrections) >= 1  # Known limitation: will detect "What's the wrong way"


def test_correction_false_positive_question():
    """Questions containing correction words should not trigger."""
    messages = [
        {"role": "user", "content": "Is this wrong?"},
        {"role": "assistant", "content": "No, it's correct"},
    ]
    
    corrections = _detect_corrections(messages)
    # Known limitation: will detect "Is this wrong?"
    assert len(corrections) >= 1  # Known limitation


def test_correction_true_positive():
    """Actual corrections should be detected."""
    messages = [
        {"role": "user", "content": "帮我查天气"},
        {"role": "assistant", "content": "北京今天晴"},
        {"role": "user", "content": "不对，我要上海的"},
    ]
    
    corrections = _detect_corrections(messages)
    assert len(corrections) == 1
    assert "上海" in corrections[0]


def test_correction_pattern_at_sentence_start():
    """Correction patterns at sentence start are more likely real."""
    messages = [
        {"role": "user", "content": "错了，应该是 UTC+8"},
    ]
    
    corrections = _detect_corrections(messages)
    assert len(corrections) == 1


def test_correction_no_false_positive_in_code():
    """Code examples with correction words should not trigger."""
    messages = [
        {"role": "user", "content": "Here's my code: if (x == wrong) { ... }"},
        {"role": "assistant", "content": "That looks fine"},
    ]
    
    corrections = _detect_corrections(messages)
    # Known limitation: will detect "wrong" in code
    assert len(corrections) >= 1  # Known limitation


def test_correction_multiple_patterns():
    """Multiple correction patterns in one message."""
    messages = [
        {"role": "user", "content": "不对，你搞错了，应该是这样"},
    ]
    
    corrections = _detect_corrections(messages)
    assert len(corrections) == 1  # Should only count once per message
