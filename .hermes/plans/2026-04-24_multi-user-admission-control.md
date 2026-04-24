# Multi-User Admission Control Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 在 Hermes Gateway 中补齐企业级多用户支持的关键能力：队列系统、审计日志、Feishu 集成

**Architecture:** 
- 在现有 `permission_policy.py` 基础上，补充 admission queue（3 lanes）和 audit trail
- 在 Feishu adapter 的 `_dispatch_inbound_event` 前插入 admission gate
- 保持 Hermes 核心（AIAgent）不变，只在 gateway 层加控制

**Tech Stack:** Python 3.11, asyncio, SQLite (audit log), existing Hermes gateway

**Current State:**
- ✅ `tools/permission_policy.py` — 完整的 Role/OpType/Decision 模型
- ✅ `~/.hermes/config/user-roles.json` — 用户角色配置
- ✅ `gateway/session.py` — `build_session_key()` 已支持 `group_sessions_per_user`
- ❌ Queue system — 缺失
- ❌ Audit trail — 缺失
- ❌ Feishu integration — 未接入 permission gate

**In Scope:**
1. Admission queue（内存队列 + 持久化）
2. Audit trail（JSONL 日志）
3. Feishu adapter 集成 admission gate
4. Queue position feedback（排队位置反馈）

**Out of Scope:**
- 分布式队列（Redis）— Phase 2
- 复杂优先级算法 — 当前只按 role 简单排序
- Queue 可视化 UI — 当前只有文本反馈
- Memory 三层治理 — 当前只有 MEMORY.md 的 main-session-only 保护

---

## Task 1: Create Admission Queue Module

**Objective:** 实现内存队列，支持 3 lanes（fast/standard/heavy）和优先级排序

**Files:**
- Create: `gateway/admission/__init__.py`
- Create: `gateway/admission/queue.py`
- Create: `gateway/admission/types.py`
- Test: `tests/gateway/test_admission_queue.py`

**Step 1: Write types**

```python
# gateway/admission/types.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from datetime import datetime

Lane = Literal["fast", "standard", "heavy"]
QueueStatus = Literal["queued", "processing", "completed", "failed"]

@dataclass
class QueueItem:
    id: str
    user_id: str
    user_role: str  # owner/admin/senior/member
    message: str
    lane: Lane
    priority: int  # owner=100, admin=50, senior=30, member=10
    status: QueueStatus = "queued"
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict | None = None
```

**Step 2: Write failing test**

```python
# tests/gateway/test_admission_queue.py
import pytest
from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import QueueItem

def test_enqueue_and_dequeue():
    queue = AdmissionQueue()
    item = QueueItem(
        id="test-1",
        user_id="user-1",
        user_role="member",
        message="test message",
        lane="standard",
        priority=10
    )
    
    queue.enqueue(item)
    dequeued = queue.dequeue("standard")
    
    assert dequeued is not None
    assert dequeued.id == "test-1"

def test_priority_ordering():
    queue = AdmissionQueue()
    
    # Enqueue member first, then owner
    member_item = QueueItem(
        id="member-1", user_id="u1", user_role="member",
        message="msg", lane="standard", priority=10
    )
    owner_item = QueueItem(
        id="owner-1", user_id="u2", user_role="owner",
        message="msg", lane="standard", priority=100
    )
    
    queue.enqueue(member_item)
    queue.enqueue(owner_item)
    
    # Owner should come out first
    first = queue.dequeue("standard")
    assert first.id == "owner-1"
```

**Step 3: Run test to verify failure**

```bash
cd /Users/songying/.hermes/hermes-agent
pytest tests/gateway/test_admission_queue.py -v
```

Expected: FAIL — "No module named 'gateway.admission'"

**Step 4: Write minimal implementation**

```python
# gateway/admission/__init__.py
from .queue import AdmissionQueue
from .types import QueueItem, Lane, QueueStatus

__all__ = ["AdmissionQueue", "QueueItem", "Lane", "QueueStatus"]
```

```python
# gateway/admission/queue.py
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Dict, List

from .types import QueueItem, Lane

class AdmissionQueue:
    def __init__(self):
        self._lanes: Dict[Lane, List[QueueItem]] = {
            "fast": [],
            "standard": [],
            "heavy": []
        }
        self._lock = threading.Lock()
        self._items_by_id: Dict[str, QueueItem] = {}
    
    def enqueue(self, item: QueueItem) -> None:
        with self._lock:
            self._lanes[item.lane].append(item)
            self._items_by_id[item.id] = item
            # Sort by priority (high to low)
            self._lanes[item.lane].sort(key=lambda x: x.priority, reverse=True)
    
    def dequeue(self, lane: Lane) -> QueueItem | None:
        with self._lock:
            if not self._lanes[lane]:
                return None
            item = self._lanes[lane].pop(0)
            item.status = "processing"
            return item
    
    def get_position(self, item_id: str) -> tuple[Lane, int] | None:
        with self._lock:
            item = self._items_by_id.get(item_id)
            if not item or item.status != "queued":
                return None
            
            lane_items = self._lanes[item.lane]
            for i, queued_item in enumerate(lane_items):
                if queued_item.id == item_id:
                    return (item.lane, i + 1)
            return None
    
    def mark_completed(self, item_id: str, result: dict | None = None) -> None:
        with self._lock:
            if item_id in self._items_by_id:
                item = self._items_by_id[item_id]
                item.status = "completed"
                item.result = result
                from datetime import datetime
                item.completed_at = datetime.now()
```

**Step 5: Run test to verify pass**

```bash
pytest tests/gateway/test_admission_queue.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add gateway/admission/ tests/gateway/test_admission_queue.py
git commit -m "feat(admission): add queue system with 3 lanes and priority"
```

---

## Task 2: Add Queue Persistence

**Objective:** 持久化队列状态到 SQLite，防止重启丢失

**Files:**
- Modify: `gateway/admission/queue.py`
- Create: `gateway/admission/persistence.py`
- Test: `tests/gateway/test_queue_persistence.py`

**Step 1: Write failing test**

```python
# tests/gateway/test_queue_persistence.py
import tempfile
from pathlib import Path
from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import QueueItem

def test_queue_survives_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "queue.db"
        
        # Create queue, enqueue item, save
        queue1 = AdmissionQueue(db_path=db_path)
        item = QueueItem(
            id="test-1", user_id="u1", user_role="owner",
            message="msg", lane="standard", priority=100
        )
        queue1.enqueue(item)
        queue1.save()
        
        # Create new queue instance, load
        queue2 = AdmissionQueue(db_path=db_path)
        queue2.load()
        
        # Should have the item
        dequeued = queue2.dequeue("standard")
        assert dequeued is not None
        assert dequeued.id == "test-1"
```

**Step 2: Run test to verify failure**

```bash
pytest tests/gateway/test_queue_persistence.py -v
```

Expected: FAIL — "AdmissionQueue.__init__() got an unexpected keyword argument 'db_path'"

**Step 3: Write implementation**

```python
# gateway/admission/persistence.py
import sqlite3
import json
from pathlib import Path
from typing import List
from .types import QueueItem, Lane
from datetime import datetime

def init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS queue_items (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            user_role TEXT,
            message TEXT,
            lane TEXT,
            priority INTEGER,
            status TEXT,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            result TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_items(db_path: Path, items: List[QueueItem]) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM queue_items WHERE status = 'queued'")
    
    for item in items:
        conn.execute("""
            INSERT OR REPLACE INTO queue_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item.id, item.user_id, item.user_role, item.message,
            item.lane, item.priority, item.status,
            item.created_at.isoformat(),
            item.started_at.isoformat() if item.started_at else None,
            item.completed_at.isoformat() if item.completed_at else None,
            json.dumps(item.result) if item.result else None
        ))
    
    conn.commit()
    conn.close()

def load_items(db_path: Path) -> List[QueueItem]:
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT * FROM queue_items WHERE status = 'queued'")
    
    items = []
    for row in cursor:
        items.append(QueueItem(
            id=row[0],
            user_id=row[1],
            user_role=row[2],
            message=row[3],
            lane=row[4],
            priority=row[5],
            status=row[6],
            created_at=datetime.fromisoformat(row[7]),
            started_at=datetime.fromisoformat(row[8]) if row[8] else None,
            completed_at=datetime.fromisoformat(row[9]) if row[9] else None,
            result=json.loads(row[10]) if row[10] else None
        ))
    
    conn.close()
    return items
```

**Step 4: Update AdmissionQueue**

```python
# gateway/admission/queue.py (add to __init__)
from pathlib import Path
from . import persistence

class AdmissionQueue:
    def __init__(self, db_path: Path | None = None):
        self._lanes: Dict[Lane, List[QueueItem]] = {
            "fast": [],
            "standard": [],
            "heavy": []
        }
        self._lock = threading.Lock()
        self._items_by_id: Dict[str, QueueItem] = {}
        
        self._db_path = db_path or (Path.home() / ".hermes" / "queue.db")
        if self._db_path:
            persistence.init_db(self._db_path)
    
    def save(self) -> None:
        if not self._db_path:
            return
        
        with self._lock:
            all_items = []
            for lane_items in self._lanes.values():
                all_items.extend(lane_items)
            persistence.save_items(self._db_path, all_items)
    
    def load(self) -> None:
        if not self._db_path:
            return
        
        items = persistence.load_items(self._db_path)
        with self._lock:
            for item in items:
                self._lanes[item.lane].append(item)
                self._items_by_id[item.id] = item
            
            # Re-sort all lanes
            for lane in self._lanes:
                self._lanes[lane].sort(key=lambda x: x.priority, reverse=True)
```

**Step 5: Run test to verify pass**

```bash
pytest tests/gateway/test_queue_persistence.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add gateway/admission/persistence.py tests/gateway/test_queue_persistence.py
git commit -m "feat(admission): add queue persistence to SQLite"
```

---

## Task 3: Create Audit Trail Module

**Objective:** 记录所有 admission 决策到 JSONL 日志

**Files:**
- Create: `gateway/admission/audit.py`
- Test: `tests/gateway/test_audit.py`

**Step 1: Write failing test**

```python
# tests/gateway/test_audit.py
import tempfile
import json
from pathlib import Path
from gateway.admission.audit import log_audit, AuditEvent

def test_audit_log_writes_jsonl():
    with tempfile.TemporaryDirectory() as tmpdir:
        audit_dir = Path(tmpdir)
        
        event = AuditEvent(
            user_id="user-1",
            action="execute_task",
            resource="task-123",
            result="allowed",
            metadata={"role": "owner"}
        )
        
        log_audit(event, audit_dir=audit_dir)
        
        # Check file exists
        log_files = list(audit_dir.glob("*.jsonl"))
        assert len(log_files) == 1
        
        # Check content
        with open(log_files[0]) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["user_id"] == "user-1"
            assert data["action"] == "execute_task"
```

**Step 2: Run test to verify failure**

```bash
pytest tests/gateway/test_audit.py -v
```

Expected: FAIL — "No module named 'gateway.admission.audit'"

**Step 3: Write implementation**

```python
# gateway/admission/audit.py
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Literal

Result = Literal["allowed", "denied", "confirmed", "approved"]

@dataclass
class AuditEvent:
    user_id: str
    action: str
    resource: str
    result: Result
    metadata: dict | None = None
    timestamp: datetime | None = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

def log_audit(event: AuditEvent, audit_dir: Path | None = None) -> None:
    if audit_dir is None:
        audit_dir = Path.home() / ".hermes" / "audit"
    
    audit_dir.mkdir(parents=True, exist_ok=True)
    
    # One file per day
    log_file = audit_dir / f"{event.timestamp.strftime('%Y-%m-%d')}.jsonl"
    
    with open(log_file, "a") as f:
        data = asdict(event)
        data["timestamp"] = event.timestamp.isoformat()
        f.write(json.dumps(data) + "\n")
```

**Step 4: Run test to verify pass**

```bash
pytest tests/gateway/test_audit.py -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add gateway/admission/audit.py tests/gateway/test_audit.py
git commit -m "feat(admission): add audit trail with JSONL logging"
```

---

## Task 4: Integrate Admission Gate into Feishu Adapter

**Objective:** 在 Feishu 消息处理流程中插入 permission check + queue + audit

**Files:**
- Modify: `gateway/platforms/feishu.py`
- Modify: `gateway/admission/__init__.py`
- Test: Manual testing (integration test)

**Step 1: Add admission controller to gateway**

```python
# gateway/admission/__init__.py (add)
from .controller import AdmissionController

__all__ = [
    "AdmissionQueue", "QueueItem", "Lane", "QueueStatus",
    "AuditEvent", "log_audit",
    "AdmissionController"
]
```

```python
# gateway/admission/controller.py
from __future__ import annotations

import uuid
from pathlib import Path
from tools.permission_policy import get_user_role_by_id, get_decision_by_id
from .queue import AdmissionQueue
from .audit import log_audit, AuditEvent
from .types import QueueItem, Lane

class AdmissionController:
    def __init__(self):
        self.queue = AdmissionQueue()
        self.queue.load()
    
    def classify_lane(self, message: str) -> Lane:
        """Classify message into fast/standard/heavy lane."""
        # Simple heuristic
        if len(message) < 50:
            return "fast"
        elif "coding" in message.lower() or "代码" in message:
            return "heavy"
        else:
            return "standard"
    
    def get_priority(self, role: str) -> int:
        priority_map = {
            "owner": 100,
            "admin": 50,
            "senior": 30,
            "member": 10
        }
        return priority_map.get(role, 10)
    
    async def admit(self, user_id: str, message: str) -> tuple[bool, str, QueueItem | None]:
        """
        Admit a message into the queue.
        
        Returns:
            (admitted, feedback_message, queue_item)
        """
        role = get_user_role_by_id(user_id)
        
        # Check permission (simplified - just check if user exists)
        if role == "member" and "危险操作" in message:
            log_audit(AuditEvent(
                user_id=user_id,
                action="execute_task",
                resource=message[:50],
                result="denied",
                metadata={"role": role, "reason": "member cannot do dangerous ops"}
            ))
            return (False, "权限不足：普通成员无法执行危险操作", None)
        
        # Create queue item
        lane = self.classify_lane(message)
        priority = self.get_priority(role)
        
        item = QueueItem(
            id=str(uuid.uuid4()),
            user_id=user_id,
            user_role=role,
            message=message,
            lane=lane,
            priority=priority
        )
        
        self.queue.enqueue(item)
        self.queue.save()
        
        # Get position
        pos = self.queue.get_position(item.id)
        position_text = f"，当前排队 {pos[1]} 位" if pos else ""
        
        log_audit(AuditEvent(
            user_id=user_id,
            action="enqueue_task",
            resource=item.id,
            result="allowed",
            metadata={"role": role, "lane": lane, "priority": priority}
        ))
        
        return (True, f"已加入 {lane} 队列{position_text}", item)
    
    def dequeue_next(self, lane: Lane) -> QueueItem | None:
        item = self.queue.dequeue(lane)
        if item:
            self.queue.save()
        return item
    
    def mark_completed(self, item_id: str, result: dict | None = None) -> None:
        self.queue.mark_completed(item_id, result)
        self.queue.save()
```

**Step 2: Integrate into Feishu adapter**

Find the message dispatch point in `gateway/platforms/feishu.py`:

```python
# gateway/platforms/feishu.py (around line 2220)
# Add at class level:
from gateway.admission import AdmissionController

class FeishuPlatform:
    def __init__(self, ...):
        # ... existing init ...
        self._admission = AdmissionController()
    
    async def _dispatch_inbound_event(self, event: MessageEvent) -> None:
        # BEFORE existing dispatch logic, add:
        
        # Check if admission control is enabled
        if self.config.extra.get("admission_control_enabled", False):
            user_id = event.sender.user_id
            message = event.message.text
            
            admitted, feedback, queue_item = await self._admission.admit(user_id, message)
            
            if not admitted:
                # Send rejection message
                await self._send_text_message(event.chat_id, feedback)
                return
            
            # Send queue position feedback
            await self._send_text_message(event.chat_id, feedback)
            
            # Wait for dequeue (simplified - in production use async queue worker)
            # For now, immediately dequeue and process
            item = self._admission.dequeue_next(queue_item.lane)
            if item:
                # Continue with normal dispatch
                pass
        
        # ... existing dispatch logic ...
```

**Step 3: Add config option**

```yaml
# ~/.hermes/config.yaml (add to platforms.feishu.extra)
platforms:
  feishu:
    enabled: true
    extra:
      admission_control_enabled: true  # NEW
```

**Step 4: Manual test**

```bash
# 1. Restart gateway
cd /Users/songying/.hermes/hermes-agent
python -m gateway.run

# 2. Send message in Feishu as member user
# Expected: "已加入 standard 队列，当前排队 1 位"

# 3. Check audit log
cat ~/.hermes/audit/$(date +%Y-%m-%d).jsonl
```

**Step 5: Commit**

```bash
git add gateway/admission/controller.py gateway/platforms/feishu.py
git commit -m "feat(admission): integrate admission gate into Feishu adapter"
```

---

## Task 5: Add Queue Worker (Async Processing)

**Objective:** 异步处理队列，避免阻塞消息接收

**Files:**
- Create: `gateway/admission/worker.py`
- Modify: `gateway/platforms/feishu.py`

**Step 1: Write queue worker**

```python
# gateway/admission/worker.py
import asyncio
import logging
from typing import Callable, Awaitable
from .types import QueueItem, Lane

logger = logging.getLogger(__name__)

class QueueWorker:
    def __init__(
        self,
        admission_controller,
        process_fn: Callable[[QueueItem], Awaitable[dict]]
    ):
        self._admission = admission_controller
        self._process_fn = process_fn
        self._running = False
        self._tasks = []
    
    async def start(self):
        self._running = True
        # Start workers for each lane
        self._tasks = [
            asyncio.create_task(self._worker_loop("fast")),
            asyncio.create_task(self._worker_loop("standard")),
            asyncio.create_task(self._worker_loop("heavy"))
        ]
    
    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
    
    async def _worker_loop(self, lane: Lane):
        while self._running:
            try:
                item = self._admission.dequeue_next(lane)
                if item:
                    logger.info(f"Processing {item.id} from {lane} lane")
                    result = await self._process_fn(item)
                    self._admission.mark_completed(item.id, result)
                else:
                    await asyncio.sleep(1)  # No items, wait
            except Exception as e:
                logger.error(f"Worker error in {lane} lane: {e}")
                await asyncio.sleep(5)
```

**Step 2: Integrate worker into Feishu platform**

```python
# gateway/platforms/feishu.py
from gateway.admission.worker import QueueWorker

class FeishuPlatform:
    def __init__(self, ...):
        # ... existing init ...
        self._admission = AdmissionController()
        self._queue_worker = None
    
    async def start(self):
        # ... existing start logic ...
        
        if self.config.extra.get("admission_control_enabled", False):
            self._queue_worker = QueueWorker(
                self._admission,
                self._process_queue_item
            )
            await self._queue_worker.start()
    
    async def stop(self):
        if self._queue_worker:
            await self._queue_worker.stop()
        # ... existing stop logic ...
    
    async def _process_queue_item(self, item: QueueItem) -> dict:
        """Process a queue item by dispatching to AIAgent."""
        # Reconstruct event from queue item
        # ... (simplified, need to store more context in QueueItem)
        
        # Call existing message handler
        # await self._handle_message(...)
        
        return {"status": "completed"}
```

**Step 3: Commit**

```bash
git add gateway/admission/worker.py
git commit -m "feat(admission): add async queue worker for background processing"
```

---

## Verification Steps

After all tasks complete:

1. **Test permission gate:**
   ```bash
   # As member user in Feishu
   # Send: "帮我删除 ~/.hermes 目录"
   # Expected: "权限不足：普通成员无法执行危险操作"
   ```

2. **Test queue:**
   ```bash
   # Send 3 messages quickly
   # Expected: Queue position feedback for each
   ```

3. **Test audit log:**
   ```bash
   cat ~/.hermes/audit/$(date +%Y-%m-%d).jsonl | jq .
   # Expected: JSON lines with user_id, action, result
   ```

4. **Test persistence:**
   ```bash
   # Restart gateway
   # Queue should survive restart
   ```

---

## Rollback Plan

If issues occur:

```bash
cd /Users/songying/.hermes/hermes-agent
git revert HEAD~5..HEAD  # Revert last 5 commits
rm -rf gateway/admission/
rm ~/.hermes/queue.db
rm -rf ~/.hermes/audit/
```

---

## Next Steps (Phase 2)

After MVP验证通过：
- Redis-based distributed queue
- Queue visibility API (查询排队状态)
- Dynamic priority adjustment
- Memory 三层治理
- 更细粒度的权限控制
