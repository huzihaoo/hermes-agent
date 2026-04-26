# Observability Module — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / 小优化（不改 API）
  │     └─ 新功能 / 新指标 / 新命令
  └─ 破坏性变更（数据格式迁移、API 签名变化）
```

---

## [1.1.1] — 2026-04-26 — Retention Policy

### Added
- **TraceStore.cleanup_old_data**：按保留天数清理 traces / spans
- **TaskStore.cleanup_old_tasks**：按保留天数清理历史任务

### Verified
- 6/6 retention policy tests passing

---

## [1.1.0] — 2026-04-26 — Dashboard UI MVP

### Added
- **Dashboard Server**：独立 FastAPI 服务 (port 8765)
- **Dashboard UI**：暗色主题，3 个 tab (Overview / Traces / Cost)
- **实时统计**：total traces, cost, avg duration, success rate
- **自动刷新**：每 30 秒刷新数据

### Files
```
gateway/observability/
├── dashboard_server.py  # FastAPI standalone server
└── static/
    └── index.html       # Dashboard UI
```

---

## [1.0.0] — 2026-04-24 — Observability MVP

### Phase A — Trace/Span Model + Pricing + CLI
- **Trace/Span 模型**：hierarchical span tree，支持 tool_call / llm_call / session 类型
- **TraceStore**：SQLite 持久化，WAL 模式
- **Pricing 计算器**：per-model token 定价，input/output 分别计费
- **CLI 命令**：`hermes trace`、`hermes cost`

### Phase B — Dashboard API
- **FastAPI mount**：`/api/traces`、`/api/traces/{id}`、`/api/cost`
- **分页 + 过滤**：按时间范围、session_id、span_type 过滤

### Phase C — Memory v2 Integration
- **Middleware**：自动为每次 LLM 调用创建 span
- **Tool tracing**：工具调用自动记录耗时和结果大小

### Phase D — Concurrency
- **线程安全**：TraceStore 使用 threading.Lock
- **批量写入**：batch insert 优化

### Phase E — Gateway Commands + E2E Tests
- **Gateway 命令**：`/trace`、`/cost` 飞书群内可用
- **E2E 测试**：完整链路测试覆盖

### Files
```
gateway/observability/
├── __init__.py      # 模块入口 + 版本号
├── trace.py         # Trace/Span 数据模型
├── store.py         # SQLite TraceStore
├── pricing.py       # Token 定价计算
├── api.py           # FastAPI Dashboard API
├── middleware.py     # 自动 tracing 中间件
└── CHANGELOG.md     # ← 本文件
```

### Verified
- tests/gateway/observability/test_trace_store.py
- tests/gateway/test_trace_cost_commands.py
- tests/hermes_cli/test_trace.py
- tests/integration/test_observability_e2e.py
- tests/run_agent/test_task_trace_events.py

---

## 升版本 Checklist

1. [ ] 确认当前版本号（`gateway/observability/__init__.py`）
2. [ ] 评估变更范围 → 决定 MAJOR/MINOR/PATCH
3. [ ] 实施变更
4. [ ] 跑测试：`pytest tests/gateway/observability/ tests/gateway/test_trace_cost_commands.py tests/hermes_cli/test_trace.py tests/integration/test_observability_e2e.py -q`
5. [ ] 更新 `__version__` 和本 CHANGELOG
6. [ ] git commit 消息包含版本号：`feat(observability): v1.x.x — 描述`

## 待办（下一版本候选）

- [ ] Prometheus metrics exporter
- [ ] Span 采样率控制
- [x] 历史数据自动清理（retention policy）(v1.1.1)
- [x] Dashboard 前端 UI (v1.1.0)
- [ ] 告警规则（cost threshold / error rate）
