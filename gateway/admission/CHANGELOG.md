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

- [ ] 边界测试：空队列 dequeue、极端 domain_id 数量、单 lane 满载
- [ ] Worker graceful shutdown 压力测试
- [ ] 持久化 connection pool（当前每次 save/load 都 open/close）
- [ ] Prometheus metrics exporter
