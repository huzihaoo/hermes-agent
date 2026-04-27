# Admission Control — 多车道准入队列

> **当前版本: v1.8.0** | [CHANGELOG](CHANGELOG.md) | [版本管理](VERSION_MANAGEMENT.md)

## 概述

多用户/多群组并发消息的准入控制与排队系统。消息按类型分入不同车道（lane），车道间并发处理，车道内 FIFO 串行。支持告警规则、策略模板、Prometheus 指标导出。

## 快速开始

### 1. 启用准入控制

编辑 `~/.hermes/config.yaml`：

```yaml
platforms:
  feishu:
    extra:
      admission_control_enabled: true
      # 可选：启用 Prometheus 指标导出
      admission_metrics_port: 9090
      # 可选：启动时自动加载策略模板
      admission_template: strict
```

### 2. 启动 Gateway

```bash
cd ~/.hermes/hermes-agent
python -m gateway.run
```

### 3. 查看队列状态

```bash
python -m gateway.admission.cli status          # 人类可读
python -m gateway.admission.cli status --json    # JSON
python -m gateway.admission.cli status --domain user  # 按域过滤
```

### 4. 管理策略模板

```bash
python -m gateway.admission.cli template seed           # 初始化内置模板
python -m gateway.admission.cli template list            # 列出所有模板
python -m gateway.admission.cli template export --name strict --path strict.json
python -m gateway.admission.cli template import --path strict.json
```

### 5. 测试

```bash
pytest tests/gateway/test_admission_*.py -q
```

当前 128/128 通过。

## 架构

```
Feishu Message
    │
    ▼
┌──────────────────────┐
│  AdmissionController  │  ← 准入决策 + 分类 + 告警
│  (controller.py)      │
│  ├─ AlertManager      │  ← 队列深度 / 错误率告警
│  └─ apply_template()  │  ← 运行时热切换策略
└────────┬─────────────┘
         │ enqueue
         ▼
┌──────────────────────┐
│   AdmissionQueue      │  ← SQLite 持久化 (ConnectionPool)
│   (queue.py)          │
│                       │
│  ┌──────┐ ┌────────┐ ┌───────┐
│  │ fast │ │standard│ │ heavy │  ← 3 车道 × 3 域
│  └──┬───┘ └───┬────┘ └──┬────┘
└─────┼─────────┼─────────┼────┘
      │         │         │
      ▼         ▼         ▼
┌────────────────────────────────┐
│       QueueWorker               │  ← per-domain dispatcher
│       (worker.py)               │     + semaphore 并发控制
└────────────────────────────────┘
         │
         ▼
    FeishuAdapter._handle_message_with_guards()
```

## 车道分类

| 车道 | 条件 | 典型场景 |
|------|------|----------|
| fast | 消息 ≤ 8 字符 | 简短回复、确认、emoji |
| heavy | 包含"代码/写代码/实现/开发"等关键词 | 编码任务、长程生成 |
| standard | 其他 | 普通问答、查询 |

## 域隔离

| 域 | 路由条件 | domain_id |
|----|----------|-----------|
| user | 默认 / 私聊 | user_id |
| group | chat_type == "group" | chat_id |
| vm | platform == "vm" 或 vm_id 存在 | vm_id |

域内 domain_id 间 round-robin 公平调度。

## 优先级

| 角色 | 优先级 | 说明 |
|------|--------|------|
| owner | 100 | 最高优先 |
| admin | 50 | 管理员 |
| senior | 30 | 高级成员 |
| member | 10 | 普通成员（默认） |

## 告警规则 (v1.1.0+)

内置两种告警规则：

- **QueueDepthAlert**: 队列深度超过 warning (10) / critical (50) 阈值时触发
- **ErrorRateAlert**: 错误率 (failed + dead) / total 超过 20% (warning) / 50% (critical) 时触发

告警支持冷却期（默认 300s）、回调投递、历史查询。

```python
from gateway.admission import AdmissionController
ctrl = AdmissionController()
history = ctrl.get_alert_history(limit=50)
```

## 策略模板 (v1.2.0+)

预定义的准入策略配置，可保存、分享、导入导出。

内置模板：
- `strict` — 严格模式（低限流、紧阈值）
- `relaxed` — 宽松模式（高限流、宽阈值）
- `vip-priority` — VIP 优先（中等限流、快速告警）

```python
from gateway.admission import AdmissionController
from gateway.admission.templates import TemplateStore

ctrl = AdmissionController()
store = TemplateStore()
store.seed_builtins()
tpl = store.get("strict")
ctrl.apply_template(tpl)
```

## Prometheus 指标 (v1.4.0+)

```python
from gateway.admission.metrics_export import MetricsExporter
exporter = MetricsExporter(ctrl)
print(exporter.export())
```

导出指标：
- `admission_total_admitted` / `rejected` / `completed` / `failed` / `retried` / `dead` (counter)
- `admission_queue_depth{domain,lane}` (gauge)
- `admission_alerts_fired_total` (counter)

## 持久化

- 队列状态：SQLite + WAL + ConnectionPool (`~/.hermes/admission/queue.db`)
- 审计日志：JSONL (`~/.hermes/audit/`)
- 策略模板：JSON (`~/.hermes/admission/templates/`)

重启后队列中未处理的消息会恢复。

## 文件结构

```
gateway/admission/
├── __init__.py           # 公开 API + 版本号
├── types.py              # QueueItem, Lane, Domain 类型定义
├── queue.py              # 内存队列 + 线程安全 + round-robin
├── controller.py         # 准入控制器（权限、限流、重试、审计、告警、模板）
├── persistence.py        # SQLite 持久化 + ConnectionPool
├── worker.py             # 异步 Worker（per-domain dispatcher）
├── audit.py              # JSONL 审计日志
├── alerts.py             # 告警规则（QueueDepthAlert / ErrorRateAlert / AlertManager）
├── templates.py          # 策略模板（PolicyTemplate / TemplateStore）
├── metrics_export.py     # Prometheus 指标导出
├── feishu_integration.py # FeishuAdapter 桥接
├── cli.py                # CLI 命令（status / clear / template）
├── README.md             # 本文件
├── CHANGELOG.md          # 变更日志
├── VERSION_MANAGEMENT.md # 版本管理规范
├── ROLLBACK.md           # 回滚 SOP
└── version_check.py      # 版本检查脚本
```

## CLI 工具

```bash
# 队列状态
python -m gateway.admission.cli status [--json] [--domain user|group|vm] [--domain-id ID]

# 清空队列（仅测试用）
python -m gateway.admission.cli clear [fast|standard|heavy]

# 策略模板管理
python -m gateway.admission.cli template list
python -m gateway.admission.cli template seed
python -m gateway.admission.cli template export --name NAME --path FILE
python -m gateway.admission.cli template import --path FILE
```

## 故障排查

### 队列卡住不处理

```bash
python -m gateway.admission.cli status
grep "Queue worker started" ~/.hermes/logs/gateway.log
python -m gateway.admission.cli clear  # 测试用
```

### 消息被拒绝

```bash
tail -f ~/.hermes/audit/$(date +%Y-%m-%d).jsonl | jq .
```

### 性能问题

```bash
grep "Completed.*in" ~/.hermes/logs/gateway.log | tail -20
```

## 设计决策

1. **三层隔离 (domain → domain_id → lane)**: 域间完全隔离，域内 domain_id 间 round-robin 公平调度
2. **默认关闭**: 通过 config flag 控制，不影响现有单用户部署
3. **错误降级**: 准入检查失败时 fall-through 到原始处理流程
4. **SQLite + ConnectionPool**: 单机部署零依赖，连接复用减少 I/O 开销
5. **0 侵入源码**: 作为独立 sidecar 部署，不修改 hermes gateway 源码
