"""
Tests for agent/session_health.py — Session memory protection.

Ref: knowledge/wiki/systems/session-memory-protection.md
"""
import time
import pytest
from agent.session_health import SessionHealthMonitor, HealthLevel, HealthCheck


class TestHealthCheck:
    """Test HealthCheck dataclass and properties."""
    
    def test_green_level_no_warnings(self):
        h = HealthCheck(level=HealthLevel.GREEN, message_count=100)
        assert not h.should_warn
        assert not h.should_block
        assert h.format_warning() is None
    
    def test_yellow_level_should_warn(self):
        h = HealthCheck(
            level=HealthLevel.YELLOW,
            message_count=401,
            warnings=["Messages: 401 ≥ 400"]
        )
        assert h.should_warn
        assert not h.should_block
        formatted = h.format_warning()
        assert formatted is not None
        assert "YELLOW" in formatted
        assert "401" in formatted
    
    def test_red_level_should_warn(self):
        h = HealthCheck(
            level=HealthLevel.RED,
            message_count=451,
            warnings=["Messages: 451 ≥ 450"]
        )
        assert h.should_warn
        assert not h.should_block
        assert "RED" in h.format_warning()
    
    def test_blocked_level_should_block(self):
        h = HealthCheck(
            level=HealthLevel.BLOCKED,
            message_count=500,
            warnings=["Messages: 500 ≥ 500 (LIMIT)"]
        )
        # BLOCKED doesn't set should_warn (only YELLOW/RED do)
        # but it does set should_block
        assert not h.should_warn
        assert h.should_block
        assert "BLOCKED" in h.format_warning()
    
    def test_status_emoji(self):
        assert HealthCheck(level=HealthLevel.GREEN).status_emoji == "🟢"
        assert HealthCheck(level=HealthLevel.YELLOW).status_emoji == "🟡"
        assert HealthCheck(level=HealthLevel.RED).status_emoji == "🔴"
        assert HealthCheck(level=HealthLevel.BLOCKED).status_emoji == "⛔"


class TestSessionHealthMonitor:
    """Test SessionHealthMonitor thresholds and tracking."""
    
    def test_init(self):
        m = SessionHealthMonitor("test_session")
        assert m.session_id == "test_session"
        assert m.large_return_count == 0
        assert m.start_time > 0
    
    def test_message_count_green(self):
        m = SessionHealthMonitor("test")
        h = m.check([{}] * 100)
        assert h.level == HealthLevel.GREEN
        assert h.message_count == 100
        assert len(h.warnings) == 0
    
    def test_message_count_yellow(self):
        m = SessionHealthMonitor("test")
        h = m.check([{}] * 401)
        assert h.level == HealthLevel.YELLOW
        assert h.message_count == 401
        assert any("400" in w for w in h.warnings)
    
    def test_message_count_red(self):
        m = SessionHealthMonitor("test")
        h = m.check([{}] * 451)
        assert h.level == HealthLevel.RED
        assert h.message_count == 451
        assert any("450" in w for w in h.warnings)
    
    def test_message_count_blocked(self):
        m = SessionHealthMonitor("test")
        h = m.check([{}] * 500)
        assert h.level == HealthLevel.BLOCKED
        assert h.message_count == 500
        assert any("LIMIT" in w for w in h.warnings)
    
    def test_runtime_thresholds(self):
        m = SessionHealthMonitor("test")
        
        # Simulate 3h runtime (green)
        m.start_time = time.time() - (3 * 3600)
        h = m.check([{}] * 10)
        assert h.level == HealthLevel.GREEN
        
        # Simulate 4.5h runtime (yellow)
        m.start_time = time.time() - (4.5 * 3600)
        h = m.check([{}] * 10)
        assert h.level == HealthLevel.YELLOW
        assert any("4h" in w for w in h.warnings)
        
        # Simulate 5.5h runtime (red)
        m.start_time = time.time() - (5.5 * 3600)
        h = m.check([{}] * 10)
        assert h.level == HealthLevel.RED
        assert any("5h" in w for w in h.warnings)
        
        # Simulate 6.5h runtime (blocked)
        m.start_time = time.time() - (6.5 * 3600)
        h = m.check([{}] * 10)
        assert h.level == HealthLevel.BLOCKED
        assert any("6h" in w for w in h.warnings)
    
    def test_combined_thresholds_max_level_wins(self):
        """When multiple thresholds trigger, highest level wins."""
        m = SessionHealthMonitor("test")
        m.start_time = time.time() - (4.5 * 3600)  # yellow runtime
        h = m.check([{}] * 451)  # red message count
        assert h.level == HealthLevel.RED
        assert len(h.warnings) == 2  # both warnings present
    
    def test_tool_return_tracking_small(self):
        m = SessionHealthMonitor("test")
        result = "x" * (30 * 1024)  # 30KB
        warning = m.track_tool_return(result)
        assert warning is None  # below 50KB threshold
        assert m.large_return_count == 0  # below 40KB threshold
    
    def test_tool_return_tracking_large(self):
        m = SessionHealthMonitor("test")
        result = "x" * (45 * 1024)  # 45KB
        warning = m.track_tool_return(result)
        assert warning is None  # below 100KB user-facing threshold
        assert m.large_return_count == 1  # above 40KB accumulation threshold
    
    def test_tool_return_tracking_huge(self):
        m = SessionHealthMonitor("test")
        result = "x" * (110 * 1024)  # 110KB
        warning = m.track_tool_return(result)
        assert warning is not None
        assert "110KB" in warning
        assert m.large_return_count == 1
    
    def test_tool_return_accumulation(self):
        m = SessionHealthMonitor("test")
        # Accumulate 5 large returns
        for _ in range(5):
            m.track_tool_return("x" * (45 * 1024))
        h = m.check([{}] * 10)
        assert h.level == HealthLevel.YELLOW
        assert any("Large tool returns" in w for w in h.warnings)
    
    def test_warning_deduplication(self):
        m = SessionHealthMonitor("test")
        h1 = m.check([{}] * 401)
        assert m.should_emit_warning(h1)  # first warning
        
        h2 = m.check([{}] * 410)
        assert not m.should_emit_warning(h2)  # within 50-message window
        
        h3 = m.check([{}] * 451)
        assert m.should_emit_warning(h3)  # crossed 50-message boundary
    
    def test_track_tool_return_non_string(self):
        m = SessionHealthMonitor("test")
        assert m.track_tool_return(None) is None
        assert m.track_tool_return(123) is None
        assert m.track_tool_return([]) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
