# Admission Control — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。每次大范围改动必须升版本号。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / 并发修复 / 小优化（不改 API）
  │     └─ 新功能 / 新 lane / 新 domain / 新配置项
  └─ 破坏性变更（数据格式迁移、API 签名变化）
```

---

## [1.5.0] — 2026-04-26 — 运维面板闭环

### Added
- **README 全面更新**：版本号 → v1.5.0，文件结构、告警/模板/Prometheus/CLI 全部补齐
- **CLI `alerts` 子命令**：查看告警历史 (`python -m gateway.admission.cli alerts`)
- **CLI `apply` 子命令**：从 store 加载模板并应用 (`python -m gateway.admission.cli apply strict`)
- **MetricsServer**：独立 HTTP server 暴露 `/metrics` 端点（stdlib http.server，零侵入 gateway）

### Verified
- 新增 `test_admission_cli_ext.py`（4 tests）+ `test_admission_metrics_server.py`（3 tests）
- admission 全量测试 135/135 通过

---

## [1.4.0] — 2026-04-26 — Prometheus metrics exporter

### Added
- **MetricsExporter**：新增 `metrics_export.py`，纯字符串生成 Prometheus exposition format，无外部依赖
- 导出 counter：admitted / rejected / completed / failed / retried / dead
- 导出 gauge：`admission_queue_depth{domain,lane}` 每个 domain×lane 组合
- 导出 counter：`admission_alerts_fired_total`

### Verified
- 新增 `test_admission_metrics_export.py`（5 tests）
- admission 全量测试 128/128 通过

---

## [1.3.0] — 2026-04-26 — 边界测试 + Shutdown 压力 + Connection Pool

### Added
- **边界测试**：空队列全 lane/domain dequeue、100 domain_id 极端数量、1000 item 单 lane 满载、backoff 全阻塞、cleanup 边界
- **Graceful shutdown 压力测试**：drain 内完成、超时取消、空队列快速停止、double stop、10 并发 in-flight drain
- **ConnectionPool**：`persistence.py` 新增 `ConnectionPool` 类，模块级 pool 缓存，所有 save/load 复用同一连接，不再每次 open/close

### Verified
- 新增 `test_admission_boundary.py`（18 tests）+ `test_admission_shutdown.py`（5 tests）+ `test_admission_connpool.py`（5 tests）
- admission 全量测试 123/123 通过

---

## [1.2.0] — 2026-04-26 — 策略模板

### Added
- **PolicyTemplate**：可复用的准入策略配置数据类（rate limit / depth threshold / error rate）
- **TemplateStore**：JSON 文件持久化，支持 CRUD / import / export / seed
- **Built-in templates**：`strict`（严格）/ `relaxed`（宽松）/ `vip-priority`（VIP 优先）
- **AdmissionController.apply_template()**：运行时热切换策略模板
- **CLI `template` 子命令**：`list` / `seed` / `export` / `import`

### Verified
- 新增 `tests/gateway/test_admission_templates.py` + `tests/gateway/test_admission_cli.py`
- admission 全量测试 95/95 通过

---

## [1.1.0] — 2026-04-26 — 告警规则

### Added
- **Alert framework**：新增 `alerts.py`，包含 `AlertManager`、`AlertRecord`、`AlertLevel`
- **Queue depth alert**：支持 warning / critical 双阈值告警
- **Error rate alert**：基于 `total_completed + total_failed + total_dead` 计算错误率并告警
- **Cooldown suppression**：同一规则支持冷却期，避免重复刷屏
- **Alert callbacks + history**：支持回调投递与历史查询
- **Controller integration**：`AdmissionController` 内置告警管理，队列深度检查改走统一 alert pipeline

### Verified
- 新增 `tests/gateway/test_admission_alerts.py`
- 17/17 告警测试通过
- admission 全量测试 79/79 通过

---

## [1.0.1] — 2026-04-25 — 并发硬化

### Fixed
- **`_gc_empty_domain_id` 竞态**：`defaultdict` 删除后并发 enqueue 重建 entry，用 `pop(key, None)` 防 KeyError
- **`_metrics` 非原子递增**：新增 `_metrics_lock`，所有 `+=` 操作加锁
- **persistence DELETE 全表重写**：改用 `INSERT OR REPLACE` upsert，消除并发写丢失

### Verified
- 62/62 测试通过（含 3 个并发压力测试）
- `test_concurrent_enqueue_no_data_loss`：10 线程 × 50 items = 500，0 丢失
- `test_concurrent_dequeue_no_duplicate_items`：5 worker 并发出队，0 重复
- `test_concurrent_enqueue_dequeue_mixed`：5 producer + 3 consumer，100% 一致性

---

## [1.0.0] — 2026-04-24 — 初始发布

### Added
- **三层隔离**：domain → domain_id → lane
- **三条车道**：fast（短消息）/ standard（标准）/ heavy（编码/部署）
- **三个域**：user（私聊）/ group（群聊）/ vm（虚拟机）
- **Round-robin 调度**：domain_id 间公平轮转
- **优先级**：owner(100) > admin(50) > senior(30) > member(10)
- **限流**：滑动窗口 per-user rate limit
- **重试**：指数退避 + 死信队列
- **持久化**：SQLite + WAL 模式
- **审计**：JSON Lines 审计日志
- **异步 Worker**：per-domain dispatcher + semaphore 并发控制
- **飞书集成**：`FeishuAdmissionBridge` 接入 gateway adapter
- **CLI**：`/queue` 状态查看 + `/stats` 统计

### Files
```
gateway/admission/
├── __init__.py          # 模块入口 + 版本号
├── types.py             # Lane/Domain/QueueItem 数据类型
├── queue.py             # 内存队列 + 线程安全
├── controller.py        # 准入控制器（权限、限流、重试、审计）
├── persistence.py       # SQLite 持久化（WAL + upsert）
├── worker.py            # 异步 Worker（per-domain dispatcher）
├── audit.py             # 审计日志
├── feishu_integration.py # 飞书适配器桥接
├── cli.py               # CLI 命令
├── README.md            # 使用文档
└── CHANGELOG.md         # ← 本文件
```

---

## 升版本 Checklist

每次修改 admission 模块前：

1. [ ] 确认当前版本号（`gateway/admission/__init__.py`）
2. [ ] 评估变更范围 → 决定 MAJOR/MINOR/PATCH
3. [ ] 实施变更
4. [ ] 跑完整测试：`pytest tests/gateway/test_admission_*.py -q`
5. [ ] 更新 `__version__` 和本 CHANGELOG
6. [ ] git commit 消息包含版本号：`feat(admission): v1.x.x — 描述`

## 待办（下一版本候选）

- [x] 边界测试：空队列 dequeue、极端 domain_id 数量、单 lane 满载 *(v1.3.0)*
- [x] Worker graceful shutdown 压力测试 *(v1.3.0)*
- [x] 持久化 connection pool（当前每次 save/load 都 open/close）*(v1.3.0)*
- [x] Prometheus metrics exporter *(v1.4.0)*
