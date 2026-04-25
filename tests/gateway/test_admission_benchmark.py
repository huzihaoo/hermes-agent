"""Performance benchmark for admission queue system."""

import asyncio
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest

from gateway.admission import AdmissionController
from gateway.admission.worker import QueueWorker


@pytest.mark.asyncio
async def test_throughput_benchmark():
    """Benchmark: measure throughput with 3 concurrent lanes."""
    with TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )
        
        processed = []
        
        async def mock_process(item):
            """Fast mock processor."""
            processed.append(item.id)
            await asyncio.sleep(0.01)  # Simulate 10ms processing
            return {"status": "completed"}
        
        worker = QueueWorker(ctrl, mock_process)
        
        # Enqueue 30 items (10 per lane)
        start = time.time()
        
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            for i in range(10):
                await ctrl.admit(f"user{i}", "hi", platform="test")  # fast
            for i in range(10, 20):
                await ctrl.admit(f"user{i}", "帮我查一下这个问题的详细原因", platform="test")  # standard
            for i in range(20, 30):
                await ctrl.admit(f"user{i}", "帮我写代码实现排序算法", platform="test")  # heavy
        
        enqueue_time = time.time() - start
        
        # Start worker and process all
        await worker.start()
        
        # Wait for all to complete
        process_start = time.time()
        while len(processed) < 30:
            await asyncio.sleep(0.1)
            if time.time() - process_start > 10:  # 10s timeout
                break
        
        process_time = time.time() - process_start
        await worker.stop()
        
        # Verify all processed
        assert len(processed) == 30
        
        # Calculate metrics
        total_time = enqueue_time + process_time
        throughput = 30 / process_time
        
        print(f"\n=== Performance Benchmark ===")
        print(f"Items: 30 (10 per lane)")
        print(f"Enqueue time: {enqueue_time:.3f}s")
        print(f"Process time: {process_time:.3f}s")
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {throughput:.1f} items/sec")
        print(f"Avg latency: {process_time/30*1000:.1f}ms per item")
        
        # Assertions
        assert process_time < 2.0, f"Processing 30 items took {process_time:.2f}s (expected <2s with 3 parallel lanes)"
        assert throughput > 15, f"Throughput {throughput:.1f} items/sec too low (expected >15)"


@pytest.mark.asyncio
async def test_queue_depth_under_load():
    """Test queue depth stays bounded under continuous load."""
    with TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )
        
        async def mock_process(item):
            await asyncio.sleep(0.05)  # 50ms processing
            return {"status": "completed"}
        
        worker = QueueWorker(ctrl, mock_process)
        await worker.start()
        
        max_depth = 0
        depths = []
        
        # Enqueue items continuously for 1 second
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            start = time.time()
            count = 0
            while time.time() - start < 1.0:
                await ctrl.admit(f"user{count}", "帮我查一下问题", platform="test")
                count += 1
                
                # Sample queue depth
                status = ctrl.get_status()
                depth = sum(s["pending"] for s in status.values() if isinstance(s, dict) and "pending" in s)
                depths.append(depth)
                max_depth = max(max_depth, depth)
                
                await asyncio.sleep(0.02)  # 20ms between enqueues
        
        # Wait for queue to drain
        await asyncio.sleep(2)
        await worker.stop()
        
        print(f"\n=== Queue Depth Under Load ===")
        print(f"Items enqueued: {count}")
        print(f"Max queue depth: {max_depth}")
        print(f"Avg queue depth: {sum(depths)/len(depths):.1f}")
        
        # Queue should stay bounded (not grow unbounded)
        assert max_depth < 20, f"Queue depth {max_depth} too high (expected <20)"


@pytest.mark.asyncio
async def test_priority_ordering_performance():
    """Test that priority ordering doesn't significantly impact throughput."""
    with TemporaryDirectory() as tmpdir:
        ctrl = AdmissionController(
            db_path=Path(tmpdir) / "q.db",
            audit_dir=Path(tmpdir) / "audit",
        )
        
        processed_order = []
        
        async def mock_process(item):
            processed_order.append((item.user_id, item.priority))
            await asyncio.sleep(0.01)
            return {"status": "completed"}
        
        worker = QueueWorker(ctrl, mock_process)
        
        # Enqueue 20 items with mixed priorities
        with patch("gateway.admission.controller._resolve_role") as mock_role:
            for i in range(20):
                # Alternate between member (10) and owner (100)
                role = "owner" if i % 2 == 0 else "member"
                mock_role.return_value = role
                await ctrl.admit(f"user{i}", "帮我查一下问题的原因", platform="test")
        
        start = time.time()
        await worker.start()
        
        # Wait for all to complete
        while len(processed_order) < 20:
            await asyncio.sleep(0.1)
            if time.time() - start > 5:
                break
        
        elapsed = time.time() - start
        await worker.stop()
        
        print(f"\n=== Priority Ordering Performance ===")
        print(f"Items: 20 (mixed priorities)")
        print(f"Time: {elapsed:.3f}s")
        print(f"Throughput: {20/elapsed:.1f} items/sec")
        
        # Verify high priority items were processed first
        first_10 = processed_order[:10]
        high_priority_count = sum(1 for _, pri in first_10 if pri == 100)
        print(f"High priority in first 10: {high_priority_count}/10")
        
        # At least 60% of first 10 should be high priority
        assert high_priority_count >= 6, "Priority ordering not working effectively"
        
        # Overall throughput should still be good
        assert elapsed < 1.0, f"Processing took {elapsed:.2f}s (expected <1s)"
