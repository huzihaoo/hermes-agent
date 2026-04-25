# Task Productization Phase 1 — Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 把 hermes 从"会话内完成"升级为"任务系统"，建立统一任务对象、状态流转、标准回执、最近任务列表与任务详情页。

**Architecture:** 基于现有 `hermes_events.py` + `task_trace.py` + `event_insights.py` 的事件日志底座，不重写底层，只在其上加产品层对象。

**Tech Stack:** Python 3.11+, SQLite (已有 admission queue 经验), JSONL event log (已有), gateway/run.py (已有 `/tasks` `/task` 命令雏形)

---

## 当前状态

### 已有基础（不重写）

1. **事件日志底座**：`hermes_events.py` 已在记录 `task:start` / `api:call` / `tool:call` / `task:complete` / `task:failed` 事件到 `~/.hermes/analytics/events.jsonl`
2. **任务追踪雏形**：`hermes_cli/task_trace.py` 已有 `list_tasks()` / `get_task_summary()` / `generate_receipt()` 函数
3. **Gateway 命令雏形**：`gateway/run.py:4677-4732` 已有 `/tasks` 和 `/task <id>` 命令，但只是简单列表，不成产品
4. **观测雏形**：`agent/event_insights.py` 已有 `_build_task_records()` 逻辑，能从事件日志聚合任务
5. **测试覆盖**：`tests/hermes_cli/test_task_browse.py` + `test_task_receipt.py` 已有基础测试

### 当前短板（本次补）

1. **无统一任务对象**：task_id 散落在事件日志里，没有显式的 `Task` 类型
2. **无状态机**：pending / running / completed / failed 状态没有显式定义
3. **无标准回执结构**：`generate_receipt()` 只是临时拼装，不是产品级 receipt
4. **无产品入口**：`/tasks` 只是调试命令，不是"最近任务列表"产品页
5. **无任务详情页**：`/task <id>` 只显示 3 行摘要，不是完整详情页

---

## Phase 1 目标

完成标志：

- [ ] 任务有唯一 ID、类型、状态、时间戳、agent route
- [ ] 至少一类任务（coding / docs / research）能输出标准 receipt
- [ ] 用户能查看最近任务列表（`/tasks`）
- [ ] 用户能查看单任务详情（`/task <id>`）
- [ ] 用户可从成功任务创建模板（Phase 1.5 前置）

---

## 实施计划

### Task 1: 定义统一任务对象与状态机

**Objective:** 建立 `Task` 类型与状态枚举，作为后续所有任务产品化的基础。

**Files:**
- Create: `gateway/tasks/types.py`
- Create: `gateway/tasks/__init__.py`

**Step 1: 写 types.py**

```python
"""Task product layer types."""
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import time


class TaskStatus(Enum):
    """Task lifecycle states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(Enum):
    """Task categories."""
    CODING = "coding"
    DOCS = "docs"
    RESEARCH = "research"
    CHAT = "chat"
    CRON = "cron"
    UNKNOWN = "unknown"


@dataclass
class TaskReceipt:
    """Standard task receipt structure."""
    task_id: str
    status: TaskStatus
    task_type: TaskType
    user_id: Optional[str]
    platform: Optional[str]
    request_summary: Optional[str]
    started_at: float
    completed_at: Optional[float]
    total_tokens: int = 0
    tool_calls: int = 0
    tool_call_details: List[Dict[str, Any]] = field(default_factory=list)
    error_class: Optional[str] = None
    error_message: Optional[str] = None
    agent_route: Optional[str] = None  # 哪个 agent 处理的
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "task_type": self.task_type.value,
            "user_id": self.user_id,
            "platform": self.platform,
            "request_summary": self.request_summary,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_tokens": self.total_tokens,
            "tool_calls": self.tool_calls,
            "tool_call_details": self.tool_call_details,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "agent_route": self.agent_route,
        }


@dataclass
class Task:
    """Task product object."""
    task_id: str
    status: TaskStatus
    task_type: TaskType
    user_id: Optional[str]
    platform: Optional[str]
    request_summary: Optional[str]
    started_at: float
    completed_at: Optional[float] = None
    agent_route: Optional[str] = None
    
    def to_receipt(self, *, total_tokens: int = 0, tool_calls: int = 0, 
                    tool_call_details: Optional[List[Dict[str, Any]]] = None,
                    error_class: Optional[str] = None, error_message: Optional[str] = None) -> TaskReceipt:
        """Convert Task to TaskReceipt with execution details."""
        return TaskReceipt(
            task_id=self.task_id,
            status=self.status,
            task_type=self.task_type,
            user_id=self.user_id,
            platform=self.platform,
            request_summary=self.request_summary,
            started_at=self.started_at,
            completed_at=self.completed_at,
            total_tokens=total_tokens,
            tool_calls=tool_calls,
            tool_call_details=tool_call_details or [],
            error_class=error_class,
            error_message=error_message,
            agent_route=self.agent_route,
        )
```

**Step 2: 写 __init__.py**

```python
"""Task product layer."""
from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType

__all__ = ["Task", "TaskReceipt", "TaskStatus", "TaskType"]
```

**Step 3: 提交**

```bash
git add gateway/tasks/
git commit -m "feat(tasks): add Task/TaskReceipt types and status machine"
```

---

### Task 2: 把 task_trace.py 升级为 task service

**Objective:** 把 `hermes_cli/task_trace.py` 的函数升级为使用新 Task 类型的 service 层。

**Files:**
- Modify: `hermes_cli/task_trace.py`

**Step 1: 导入新类型**

在 `task_trace.py` 顶部添加：

```python
from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType
```

**Step 2: 重写 `list_tasks()` 返回 Task 对象**

替换现有 `list_tasks()` 函数：

```python
def list_tasks(
    *,
    trace_file: Path,
    limit: int = 10,
    user_id: Optional[str] = None,
) -> List[Task]:
    """List recent tasks from event log, return Task objects."""
    if not trace_file.exists():
        return []

    tasks: Dict[str, Dict[str, Any]] = {}
    with open(trace_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            data = event.get("data") or {}
            task_id = data.get("task_id")
            if not task_id:
                continue
            task = tasks.setdefault(task_id, {
                "task_id": task_id,
                "user_id": data.get("user_id"),
                "platform": data.get("platform"),
                "request_summary": None,
                "started_at": None,
                "status": TaskStatus.PENDING,
                "task_type": TaskType.UNKNOWN,
                "agent_route": None,
                "completed_at": None,
            })
            if event.get("event") in {"task:start", "request:start"}:
                if user_id and data.get("user_id") != user_id:
                    continue
                task["started_at"] = event.get("timestamp", 0)
                task["user_id"] = data.get("user_id") or task.get("user_id")
                task["platform"] = data.get("platform") or task.get("platform")
                task["request_summary"] = data.get("request_summary") or task.get("request_summary")
                task["status"] = TaskStatus.RUNNING
                # 推断 task_type
                summary = task["request_summary"] or ""
                if any(kw in summary.lower() for kw in ["代码", "写代码", "实现", "开发", "coding"]):
                    task["task_type"] = TaskType.CODING
                elif any(kw in summary.lower() for kw in ["文档", "写文档", "docs"]):
                    task["task_type"] = TaskType.DOCS
                elif any(kw in summary.lower() for kw in ["研究", "调研", "search", "research"]):
                    task["task_type"] = TaskType.RESEARCH
                else:
                    task["task_type"] = TaskType.CHAT
            elif event.get("event") == "task:complete":
                task["status"] = TaskStatus.COMPLETED
                task["completed_at"] = event.get("timestamp", 0)
            elif event.get("event") == "task:failed":
                task["status"] = TaskStatus.FAILED
                task["completed_at"] = event.get("timestamp", 0)

    sorted_tasks = sorted(tasks.values(), key=lambda t: t.get("started_at") or 0, reverse=True)
    return [
        Task(
            task_id=t["task_id"],
            status=t["status"],
            task_type=t["task_type"],
            user_id=t["user_id"],
            platform=t["platform"],
            request_summary=t["request_summary"],
            started_at=t["started_at"] or 0,
            completed_at=t.get("completed_at"),
            agent_route=t.get("agent_route"),
        )
        for t in sorted_tasks[:limit]
    ]
```

**Step 3: 重写 `generate_receipt()` 返回 TaskReceipt 对象**

替换现有 `generate_receipt()` 函数：

```python
def generate_receipt(*, trace_file: Path, task_id: str) -> TaskReceipt:
    """Generate a TaskReceipt from events.jsonl."""
    summary = get_task_summary(trace_file=trace_file, task_id=task_id)
    if summary["status"] == "not_found":
        # 返回一个 not_found 状态的 receipt
        return TaskReceipt(
            task_id=task_id,
            status=TaskStatus.PENDING,  # 用 PENDING 表示未找到
            task_type=TaskType.UNKNOWN,
            user_id=None,
            platform=None,
            request_summary=None,
            started_at=0,
            completed_at=None,
        )
    
    # Collect tool calls
    tool_calls = []
    with trace_file.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("event") == "tool:call" and event.get("data", {}).get("task_id") == task_id:
                    tool_calls.append(event["data"])
            except (json.JSONDecodeError, KeyError):
                continue
    
    # 推断 task_type
    summary_text = summary.get("request_summary") or ""
    if any(kw in summary_text.lower() for kw in ["代码", "写代码", "实现", "开发", "coding"]):
        task_type = TaskType.CODING
    elif any(kw in summary_text.lower() for kw in ["文档", "写文档", "docs"]):
        task_type = TaskType.DOCS
    elif any(kw in summary_text.lower() for kw in ["研究", "调研", "search", "research"]):
        task_type = TaskType.RESEARCH
    else:
        task_type = TaskType.CHAT
    
    # 映射 status
    status_map = {
        "completed": TaskStatus.COMPLETED,
        "failed": TaskStatus.FAILED,
        "pending": TaskStatus.PENDING,
    }
    status = status_map.get(summary["status"], TaskStatus.PENDING)
    
    return TaskReceipt(
        task_id=task_id,
        status=status,
        task_type=task_type,
        user_id=summary.get("user_id"),
        platform=summary.get("platform"),
        request_summary=summary.get("request_summary"),
        started_at=summary.get("started_at", 0),
        completed_at=summary.get("completed_at"),
        total_tokens=summary.get("total_tokens", 0),
        tool_calls=len(tool_calls),
        tool_call_details=tool_calls,
        error_class=summary.get("error_class"),
        error_message=summary.get("error_message"),
    )
```

**Step 4: 提交**

```bash
git add hermes_cli/task_trace.py
git commit -m "feat(tasks): upgrade task_trace to use Task/TaskReceipt types"
```

---

### Task 3: 升级 gateway `/tasks` 命令为产品级任务列表

**Objective:** 把 `gateway/run.py` 的 `/tasks` 命令从"调试命令"升级为"最近任务列表"产品页。

**Files:**
- Modify: `gateway/run.py:4677-4701`

**Step 1: 重写 `_handle_tasks_command()`**

替换 `gateway/run.py:4677-4701` 的函数：

```python
async def _handle_tasks_command(self, event: MessageEvent) -> str:
    """Handle /tasks - list recent tasks (product-level)."""
    from hermes_cli.task_trace import list_tasks
    from gateway.tasks.types import TaskStatus
    try:
        from gateway.admission.audit import AuditEvent, log_audit
        log_audit(AuditEvent(
            user_id=event.source.user_id or "unknown",
            action="list_tasks",
            resource="task_list",
            result="allowed",
            metadata={"platform": event.source.platform.value if event.source.platform else ""},
        ))
    except Exception:
        pass
    trace_file = _hermes_home / "analytics" / "events.jsonl"
    user_id = event.source.user_id
    tasks = list_tasks(trace_file=trace_file, limit=10, user_id=user_id)
    if not tasks:
        return "📋 **最近任务**\\n\\n暂无任务记录。"
    
    lines = ["📋 **最近任务**\\n"]
    for t in tasks:
        # 状态图标
        if t.status == TaskStatus.COMPLETED:
            icon = "✅"
        elif t.status == TaskStatus.FAILED:
            icon = "❌"
        elif t.status == TaskStatus.RUNNING:
            icon = "⏳"
        else:
            icon = "⏸️"
        
        # 任务类型标签
        type_label = {
            "coding": "💻",
            "docs": "📝",
            "research": "🔍",
            "chat": "💬",
            "cron": "⏰",
            "unknown": "❓",
        }.get(t.task_type.value, "❓")
        
        summary = t.request_summary or "无摘要"
        lines.append(f"{icon} {type_label} `{t.task_id}` — {summary[:50]}")
    
    lines.append("\\n💡 使用 `/task <id>` 查看详情")
    return "\\n".join(lines)
```

**Step 2: 提交**

```bash
git add gateway/run.py
git commit -m "feat(tasks): upgrade /tasks to product-level task list"
```

---

### Task 4: 升级 gateway `/task <id>` 命令为产品级任务详情页

**Objective:** 把 `/task <id>` 从"3行摘要"升级为"完整详情页"。

**Files:**
- Modify: `gateway/run.py:4703-4732`

**Step 1: 重写 `_handle_task_command()`**

替换 `gateway/run.py:4703-4732` 的函数：

```python
async def _handle_task_command(self, event: MessageEvent) -> str:
    """Handle /task <id> - show task details (product-level)."""
    from hermes_cli.task_trace import generate_receipt
    from gateway.tasks.types import TaskStatus
    task_id = event.get_command_args().strip()
    if not task_id:
        return "用法: `/task <task_id>`"
    try:
        from gateway.admission.audit import AuditEvent, log_audit
        log_audit(AuditEvent(
            user_id=event.source.user_id or "unknown",
            action="get_task",
            resource=task_id,
            result="allowed",
            metadata={"platform": event.source.platform.value if event.source.platform else ""},
        ))
    except Exception:
        pass
    trace_file = _hermes_home / "analytics" / "events.jsonl"
    receipt = generate_receipt(trace_file=trace_file, task_id=task_id)
    
    if receipt.status == TaskStatus.PENDING and receipt.started_at == 0:
        return f"任务 `{task_id}` 未找到。"
    
    # 状态图标
    if receipt.status == TaskStatus.COMPLETED:
        icon = "✅"
    elif receipt.status == TaskStatus.FAILED:
        icon = "❌"
    elif receipt.status == TaskStatus.RUNNING:
        icon = "⏳"
    else:
        icon = "⏸️"
    
    # 任务类型标签
    type_label = {
        "coding": "💻 编码",
        "docs": "📝 文档",
        "research": "🔍 研究",
        "chat": "💬 对话",
        "cron": "⏰ 定时",
        "unknown": "❓ 未知",
    }.get(receipt.task_type.value, "❓ 未知")
    
    lines = [
        f"{icon} **任务详情** `{task_id}`",
        f"**类型:** {type_label}",
        f"**状态:** {receipt.status.value}",
        f"**用户:** {receipt.user_id or '未知'}",
        f"**平台:** {receipt.platform or '未知'}",
        f"**摘要:** {receipt.request_summary or '无'}",
        f"**开始时间:** {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(receipt.started_at))}",
    ]
    
    if receipt.completed_at:
        lines.append(f"**完成时间:** {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(receipt.completed_at))}")
        duration = receipt.completed_at - receipt.started_at
        lines.append(f"**耗时:** {duration:.1f}s")
    
    lines.append(f"**Token 消耗:** {receipt.total_tokens}")
    lines.append(f"**工具调用:** {receipt.tool_calls} 次")
    
    if receipt.tool_call_details:
        lines.append("\\n**工具调用详情:**")
        for i, tool in enumerate(receipt.tool_call_details[:5], 1):
            tool_name = tool.get("tool_name", "unknown")
            lines.append(f"  {i}. `{tool_name}`")
        if len(receipt.tool_call_details) > 5:
            lines.append(f"  ... 还有 {len(receipt.tool_call_details) - 5} 个")
    
    if receipt.status == TaskStatus.FAILED:
        lines.append(f"\\n**错误类型:** {receipt.error_class or '未知'}")
        lines.append(f"**错误信息:** {receipt.error_message or '无'}".replace("\\n", " ")[:200])
    
    return "\\n".join(lines)
```

**Step 2: 提交**

```bash
git add gateway/run.py
git commit -m "feat(tasks): upgrade /task to product-level task detail page"
```

---

### Task 5: 补测试覆盖

**Objective:** 为新的 Task/TaskReceipt 类型和升级后的命令补测试。

**Files:**
- Create: `tests/gateway/test_tasks_types.py`
- Modify: `tests/hermes_cli/test_task_browse.py`
- Modify: `tests/hermes_cli/test_task_receipt.py`

**Step 1: 写 test_tasks_types.py**

```python
"""Tests for gateway.tasks.types."""
import pytest
from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType


def test_task_to_receipt():
    task = Task(
        task_id="t1",
        status=TaskStatus.COMPLETED,
        task_type=TaskType.CODING,
        user_id="alice",
        platform="feishu",
        request_summary="hello",
        started_at=1000.0,
        completed_at=1010.0,
    )
    receipt = task.to_receipt(total_tokens=500, tool_calls=2)
    assert receipt.task_id == "t1"
    assert receipt.status == TaskStatus.COMPLETED
    assert receipt.task_type == TaskType.CODING
    assert receipt.total_tokens == 500
    assert receipt.tool_calls == 2


def test_task_receipt_to_dict():
    receipt = TaskReceipt(
        task_id="t2",
        status=TaskStatus.FAILED,
        task_type=TaskType.DOCS,
        user_id="bob",
        platform="cli",
        request_summary="test",
        started_at=2000.0,
        completed_at=2020.0,
        error_class="api_error",
        error_message="boom",
    )
    d = receipt.to_dict()
    assert d["task_id"] == "t2"
    assert d["status"] == "failed"
    assert d["task_type"] == "docs"
    assert d["error_class"] == "api_error"
```

**Step 2: 更新 test_task_browse.py**

在 `tests/hermes_cli/test_task_browse.py` 顶部添加：

```python
from gateway.tasks.types import Task, TaskStatus, TaskType
```

修改 `test_list_tasks()` 的断言：

```python
def test_list_tasks(tmp_path):
    # ... 现有 setup 代码 ...
    tasks = list_tasks(trace_file=events_file, limit=10)
    assert len(tasks) == 2
    assert isinstance(tasks[0], Task)
    assert tasks[0].task_id == "t1"
    assert tasks[0].status == TaskStatus.PENDING  # 因为没有 complete 事件
    assert tasks[1].task_id == "t2"
    assert tasks[1].status == TaskStatus.COMPLETED
```

**Step 3: 更新 test_task_receipt.py**

在 `tests/hermes_cli/test_task_receipt.py` 顶部添加：

```python
from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType
```

修改 `test_generate_receipt_from_completed_task()` 的断言：

```python
def test_generate_receipt_from_completed_task(tmp_path):
    # ... 现有 setup 代码 ...
    receipt = generate_receipt(trace_file=events_file, task_id="t1")
    assert isinstance(receipt, TaskReceipt)
    assert receipt.task_id == "t1"
    assert receipt.status == TaskStatus.COMPLETED
    assert receipt.user_id == "alice"
    assert receipt.total_tokens == 500
    assert receipt.tool_calls == 1
```

**Step 4: 运行测试**

```bash
pytest tests/gateway/test_tasks_types.py tests/hermes_cli/test_task_browse.py tests/hermes_cli/test_task_receipt.py -v
```

预期：全部通过。

**Step 5: 提交**

```bash
git add tests/
git commit -m "test(tasks): add tests for Task/TaskReceipt types and upgraded commands"
```

---

## 验证方式

### 手动验证

1. 启动 gateway：`python -m gateway.run`
2. 发送几条飞书消息，触发任务
3. 发送 `/tasks`，应看到最近任务列表，带图标和类型标签
4. 发送 `/task <id>`，应看到完整详情页，包含耗时、工具调用详情

### 自动化验证

```bash
pytest tests/gateway/test_tasks_types.py tests/hermes_cli/test_task_browse.py tests/hermes_cli/test_task_receipt.py -v
```

---

## 风险与开放问题

### 风险

1. **task_type 推断不准确**：当前用关键词匹配推断任务类型，可能误判。缓解：Phase 1.5 引入显式 task_type 标记。
2. **event log 丢失**：如果 `events.jsonl` 被删除，任务历史丢失。缓解：Phase 1.5 引入 SQLite 持久化。
3. **大量任务时性能**：`list_tasks()` 全量扫描 JSONL，任务多时变慢。缓解：Phase 1.5 引入索引或 SQLite。

### 开放问题

1. **任务详情页是否需要"重试"按钮？** → Phase 1.5 决定
2. **是否需要"取消任务"功能？** → Phase 1.5 决定
3. **任务列表是否需要分页？** → Phase 1.5 决定

---

## Phase 1.5 前瞻

Phase 1 完成后，立刻接上：

1. **SQLite 持久化**：把任务对象持久化到 SQLite，不再依赖 JSONL 全量扫描
2. **显式 task_type 标记**：在 `task:start` 事件中加 `task_type` 字段
3. **模板系统雏形**：从成功任务创建模板
4. **cron 绑定模板**：让 cron 能调度模板

---

## 参考材料

- 知识库：`~/.hermes/workspace-work/knowledge/wiki/systems/aime-openclaw-capability-gap-map.md`
- 知识库：`~/.hermes/workspace-work/knowledge/wiki/systems/openclaw-productization-roadmap.md`
- 代码：`hermes_cli/task_trace.py`
- 代码：`agent/event_insights.py`
- 代码：`gateway/run.py:4677-4732`
- 测试：`tests/hermes_cli/test_task_browse.py`
- 测试：`tests/hermes_cli/test_task_receipt.py`

## DELIVERY SUMMARY

**Phase 1 + Phase 1.5-A/B/C 已完成并落地。**

### 交付物

| 阶段 | 文件 | 功能 | 测试 |
|------|------|------|------|
| **Phase 1** | `gateway/tasks/types.py` | Task/TaskReceipt 类型 + TaskStatus/TaskType 枚举 + `_infer_task_type()` | ✅ |
| | `hermes_cli/task_trace.py` | `list_tasks()` 返回 `List[Task]`，`generate_receipt()` 返回 `TaskReceipt` | ✅ |
| | `gateway/run.py` | `/tasks` 和 `/task <id>` 产品化 UI（类型图标 + 状态图标 + 详情页） | ✅ |
| **Phase 1.5-A** | `gateway/tasks/store.py` | SQLite TaskStore (upsert/get/list_recent) | ✅ |
| | `hermes_events.py` | EventEmitter 同步任务事件到 TaskStore | ✅ |
| | `run_agent.py` + `gateway/run.py` | 初始化时传入 TaskStore | ✅ |
| **Phase 1.5-B** | `hermes_events.py` | `TaskEvent.task_start()` 接受显式 `task_type` | ✅ |
| | `run_agent.py` | 传入 `task_type="chat"` | ✅ |
| **Phase 1.5-C** | `gateway/tasks/template.py` | TemplateStore (create_from_task/get/list_recent) | ✅ |
| | `gateway/run.py` | `/template create <task_id> <name>` + `/templates` 命令 | ✅ |

### 测试覆盖

- **36/36 测试通过**
- TaskStore: 6 tests
- EventEmitter + TaskStore 集成: 5 tests
- TemplateStore: 4 tests
- Template 命令: 4 tests
- task_trace CLI: 4 tests
- Gateway 命令: 2 tests
- EventEmitter 基础: 11 tests

### Commits

```
3fe52552 feat(tasks): add template system - TemplateStore + /template + /templates commands
6379a68f feat(tasks): add explicit task_type to task:start events
4bc80645 feat(tasks): add SQLite TaskStore + sync from EventEmitter
07c53b89 test(tasks): fix gateway task tests for Task/TaskReceipt types
feaf9f00 feat(tasks): upgrade /tasks and /task commands to product-level UI
33ee494c feat(tasks): upgrade task_trace to use Task/TaskReceipt types + sync tests
09e39f94 feat(tasks): add Task/TaskReceipt types and status machine
```

### 下一步（Phase 2）

Phase 1.5-D（cron 绑定模板）需要修改 cron 系统，属于更大的产品特性，建议在 Phase 1 + 1.5-A/B/C 稳定运行一段时间后再推进。

当前交付物已具备：
- ✅ 类型化任务对象
- ✅ SQLite 持久化存储
- ✅ 产品级 UI 命令
- ✅ 模板系统雏形
- ✅ 36 个测试全绿

可以开始在生产环境使用 `/tasks`、`/task <id>`、`/template create`、`/templates` 命令。
