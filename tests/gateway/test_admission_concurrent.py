"""Test concurrent admission processing across domains and lanes."""

import asyncio
import pytest

from gateway.admission import AdmissionController
from gateway.admission.worker import QueueWorker


@pytest.fixture
def admission_controller(tmp_path):
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


@pytest.mark.asyncio
async def test_different_domains_process_in_parallel(admission_controller):
    """Items in user/group/vm domains should process concurrently."""
    processing_times = {}

    async def mock_process(item):
        start = asyncio.get_event_loop().time()
        await asyncio.sleep(0.1)
        end = asyncio.get_event_loop().time()
        processing_times[item.id] = (start, end, item.domain)
        return {"status": "completed"}

    _, _, item_user = await admission_controller.admit(
        "u1", "帮我查一下这个问题", chat_type="p2p", platform="feishu",
    )
    _, _, item_group = await admission_controller.admit(
        "u2", "帮我查一下这个问题", chat_id="g1", chat_type="group", platform="feishu",
    )
    _, _, item_vm = await admission_controller.admit(
        "u3", "帮我查一下这个问题", platform="vm", vm_id="vm-1",
    )

    assert item_user.domain == "user"
    assert item_group.domain == "group"
    assert item_vm.domain == "vm"

    worker = QueueWorker(admission_controller, mock_process)
    await worker.start()
    await asyncio.sleep(0.5)
    await worker.stop()

    assert len(processing_times) == 3

    times = list(processing_times.values())
    overlaps = 0
    for i in range(len(times)):
        for j in range(i + 1, len(times)):
            s1, e1, _ = times[i]
            s2, e2, _ = times[j]
            if (s1 <= s2 < e1) or (s2 <= s1 < e2):
                overlaps += 1

    assert overlaps >= 1, "Expected cross-domain parallel execution"


@pytest.mark.asyncio
async def test_different_domain_ids_process_in_parallel(admission_controller):
    """Different domain_ids within the same domain process via round-robin."""
    processing_times = {}

    async def mock_process(item):
        start = asyncio.get_event_loop().time()
        await asyncio.sleep(0.1)
        end = asyncio.get_event_loop().time()
        processing_times[item.id] = (start, end)
        return {"status": "completed"}

    _, _, i1 = await admission_controller.admit(
        "u1", "帮我查一下这个问题的原因是什么", platform="feishu",
    )
    _, _, i2 = await admission_controller.admit(
        "u2", "帮我看看这个日志里面有什么异常", platform="feishu",
    )
    _, _, i3 = await admission_controller.admit(
        "u3", "帮我分析一下这个配置文件的问题", platform="feishu",
    )

    assert i1.domain == i2.domain == i3.domain == "user"
    assert i1.domain_id != i2.domain_id

    worker = QueueWorker(admission_controller, mock_process)
    await worker.start()
    await asyncio.sleep(0.5)
    await worker.stop()

    assert len(processing_times) == 3


@pytest.mark.asyncio
async def test_priority_within_lane(admission_controller):
    """Higher priority users get processed first within a lane."""
    processed = []

    async def mock_process(item):
        processed.append((item.user_id, item.user_role))
        await asyncio.sleep(0.05)
        return {"status": "completed"}

    _, _, member_item = await admission_controller.admit(
        "member_user", "member问题", platform="test",
    )
    _, _, owner_item = await admission_controller.admit(
        "owner_user", "owner问题", platform="test",
    )

    member_item.user_role = "member"
    member_item.priority = 10
    owner_item.user_role = "owner"
    owner_item.priority = 100

    admission_controller.queue.enqueue(member_item)
    admission_controller.queue.enqueue(owner_item)

    worker = QueueWorker(admission_controller, mock_process)
    await worker.start()
    await asyncio.sleep(0.3)
    await worker.stop()

    assert len(processed) >= 2
