"""Task product layer.

Version History:
  v1.0.0 (2026-04-25) — Initial release
    - Task/TaskReceipt types with TaskStatus/TaskType enums
    - SQLite TaskStore with persistence
    - TemplateStore for task templates
    - EventEmitter integration for auto-sync
    - Gateway commands: /tasks, /task <id>, /template, /templates
    - Webhook template_id integration
    - 36/36 tests passing

  v1.1.0 (2026-04-26) — Webhook template integration
    - Webhook routes support template_id
    - Template prompt/skills as fallback
    - Template usage tracking
    - 5/5 webhook integration tests passing
"""

__version__ = "1.1.0"

from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType, _infer_task_type

__all__ = [
    "__version__",
    "Task",
    "TaskReceipt",
    "TaskStatus",
    "TaskType",
    "_infer_task_type",
]
