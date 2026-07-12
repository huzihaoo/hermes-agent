"""Task product layer types."""
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


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
    INTAKE = "intake"
    UNKNOWN = "unknown"


def _infer_task_type(summary: Optional[str]) -> TaskType:
    """Infer task type from request summary using keyword matching."""
    if not summary:
        return TaskType.UNKNOWN
    summary_lower = summary.lower()
    if any(kw in summary_lower for kw in ["代码", "写代码", "实现", "开发", "coding"]):
        return TaskType.CODING
    elif any(kw in summary_lower for kw in ["文档", "写文档", "docs"]):
        return TaskType.DOCS
    elif any(kw in summary_lower for kw in ["研究", "调研", "search", "research"]):
        return TaskType.RESEARCH
    else:
        return TaskType.CHAT


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
    chat_id: Optional[str] = None
    chat_type: Optional[str] = None
    thread_id: Optional[str] = None
    message_id: Optional[str] = None
    error_class: Optional[str] = None
    error_message: Optional[str] = None
    receipt_path: Optional[str] = None
    delivery_verified: Optional[bool] = None
    memory_write_count: Optional[int] = None
    skill_write_count: Optional[int] = None
    permission_decision: Optional[str] = None
    vm_task_id: Optional[str] = None
    delivery_receipt: Optional[str] = None

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
            error_class=error_class if error_class is not None else self.error_class,
            error_message=error_message if error_message is not None else self.error_message,
            agent_route=self.agent_route,
        )
