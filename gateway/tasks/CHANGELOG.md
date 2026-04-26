# Task Product Layer — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / 小优化（不改 API）
  │     └─ 新功能 / 新类型 / 新命令
  └─ 破坏性变更（数据格式迁移、API 签名变化）
```

---

## [1.3.0] — 2026-04-26 — Task Cancel + Retry

### Added
- **TaskStore.cancel_task**：支持取消 `pending` / `running` 状态任务
- **TaskStore.retry_task**：支持将 `failed` / `cancelled` 状态任务重置为 `pending`
- **Gateway 子命令**：`/task cancel <id>`、`/task retry <id>`
- **归属校验**：仅任务所属用户可执行取消/重试

### Verified
- 20/20 cancel+retry tests passing

---

## [1.2.0] — 2026-04-26 — Pagination + Status Filter

### Added
- **TaskStore.list_recent 分页**：新增 offset 和 status 参数
- **TaskStore.count_tasks**：按 user_id / status 统计任务总数
- **/tasks [page]**：飞书群内分页浏览任务列表（10条/页）
- **分页导航**：footer 显示上一页/下一页提示

### Verified
- 9/9 pagination tests passing
- 98/98 total core tests passing

---

## [1.1.0] — 2026-04-26 — Webhook Template Integration

### Added
- **Webhook template_id 支持**：webhook routes 可绑定 template_id
- **Template prompt fallback**：route 无 prompt 时使用模板的 request_summary
- **Template skills fallback**：route 无 skills 时使用模板的 skills
- **Template usage tracking**：webhook 触发时自动记录模板使用次数

### Verified
- 5/5 webhook template integration tests passing

---

## [1.0.0] — 2026-04-25 — 初始发布

### Added
- **Task/TaskReceipt 类型**：统一任务对象与标准回执结构
- **TaskStatus 枚举**：pending / running / completed / failed / cancelled
- **TaskType 枚举**：coding / docs / research / chat / cron / unknown
- **_infer_task_type()**：基于关键词推断任务类型
- **SQLite TaskStore**：upsert / get / list_recent 持久化
- **TemplateStore**：create_from_task / get / list_recent / record_usage
- **EventEmitter 集成**：task:start / task:complete / task:failed 自动同步到 TaskStore
- **Gateway 命令**：
  - `/tasks` — 产品级最近任务列表（类型图标 + 状态图标）
  - `/task <id>` — 完整任务详情页（耗时、工具调用、错误信息）
  - `/template create <task_id> <name>` — 从成功任务创建模板
  - `/templates` — 模板列表
- **Cron 模板绑定**：cron jobs 支持 template_id

### Files
```
gateway/tasks/
├── __init__.py     # 模块入口 + 版本号
├── types.py        # Task/TaskReceipt/TaskStatus/TaskType
├── store.py        # SQLite TaskStore
├── template.py     # TemplateStore
└── CHANGELOG.md    # ← 本文件
```

### Verified
- 36/36 tests passing
- TaskStore: 6 tests
- EventEmitter + TaskStore 集成: 5 tests
- TemplateStore: 4 tests
- Template 命令: 4 tests
- task_trace CLI: 4 tests
- Gateway 命令: 2 tests
- EventEmitter 基础: 11 tests

---

## 升版本 Checklist

1. [ ] 确认当前版本号（`gateway/tasks/__init__.py`）
2. [ ] 评估变更范围 → 决定 MAJOR/MINOR/PATCH
3. [ ] 实施变更
4. [ ] 跑测试：`pytest tests/hermes_cli/test_task_*.py tests/gateway/test_webhook_template_integration.py -q`
5. [ ] 更新 `__version__` 和本 CHANGELOG
6. [ ] git commit 消息包含版本号：`feat(tasks): v1.x.x — 描述`

## 待办（下一版本候选）

- [x] 任务列表分页 (v1.2.0)
- [x] 任务取消功能 (v1.3.0)
- [x] 任务重试功能 (v1.3.0)
- [ ] 模板市场 / 模板分享
- [ ] Dashboard UI 集成
