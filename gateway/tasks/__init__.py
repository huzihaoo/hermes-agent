"""Task product layer.

Version History:
  v1.3.0 (2026-04-26) — Task cancel + retry
    - TaskStore.cancel_task(): cancel pending/running tasks
    - TaskStore.retry_task(): reset failed/cancelled tasks to pending
    - Gateway commands: /task cancel <id>, /task retry <id>
    - Ownership check: only the task owner can cancel/retry
    - 20/20 cancel+retry tests passing

  v1.2.0 (2026-04-26) — Pagination + status filter
    - TaskStore.list_recent: offset + status filter
    - TaskStore.count_tasks: count with filters
    - /tasks [page]: paginated task list
    - 9/9 pagination tests passing

  v1.1.0 (2026-04-26) — Webhook template integration
    - Webhook routes support template_id
    - Template prompt/skills as fallback
    - Template usage tracking
    - 5/5 webhook integration tests passing

  v1.0.0 (2026-04-25) — Initial release
    - Task/TaskReceipt types with TaskStatus/TaskType enums
    - SQLite TaskStore with persistence
    - TemplateStore for task templates
    - EventEmitter integration for auto-sync
    - Gateway commands: /tasks, /task <id>, /template, /templates
    - Webhook template_id integration
    - 36/36 tests passing
"""

__version__ = "1.3.0"

from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType, _infer_task_type

__all__ = [
    "__version__",
    "Task",
    "TaskReceipt",
    "TaskStatus",
    "TaskType",
    "_infer_task_type",
]
