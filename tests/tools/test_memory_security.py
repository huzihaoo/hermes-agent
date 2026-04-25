"""Security tests for memory system."""
import pytest


def test_user_id_path_traversal_blocked():
    """user_id with path traversal should be rejected."""
    from tools.memory_tool import MemoryStore
    
    with pytest.raises(ValueError, match="path separators"):
        MemoryStore(user_id="../../../etc")
    
    with pytest.raises(ValueError, match="path separators"):
        MemoryStore(user_id="user/../admin")
    
    with pytest.raises(ValueError, match="path separators"):
        MemoryStore(user_id="user/subdir")
    
    with pytest.raises(ValueError, match="path separators"):
        MemoryStore(user_id="user\\subdir")


def test_user_id_empty_rejected():
    """Empty user_id should be rejected."""
    from tools.memory_tool import MemoryStore
    
    with pytest.raises(ValueError, match="non-empty"):
        MemoryStore(user_id="")


def test_user_id_too_long_rejected():
    """Extremely long user_id should be rejected."""
    from tools.memory_tool import MemoryStore
    
    with pytest.raises(ValueError, match="too long"):
        MemoryStore(user_id="x" * 256)


def test_user_id_valid_formats():
    """Valid user_id formats should be accepted."""
    from tools.memory_tool import MemoryStore
    
    # These should all work
    valid_ids = [
        "user123",
        "user-123",
        "user_123",
        "user.123",
        "alice@example.com",
        "user:session:123",
    ]
    
    for user_id in valid_ids:
        store = MemoryStore(user_id=user_id)
        assert store.user_id == user_id


def test_user_id_none_allowed():
    """user_id=None should be allowed (backward compat)."""
    from tools.memory_tool import MemoryStore
    
    store = MemoryStore(user_id=None)
    assert store.user_id is None
