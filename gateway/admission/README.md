# Admission Control — 多车道准入队列

## 概述

多用户/多群组并发消息的准入控制与排队系统。消息按类型分入不同车道（lane），车道间并发处理，车道内 FIFO 串行。

## 快速开始

### 1. 启用准入控制

编辑 `~/.hermes/config.yaml`：

```yaml
platforms:
  feishu:
    extra:
      admission_control_enabled: true
```

### 2. 启动 Gateway

```bash
cd ~/.hermes/hermes-agent
python -m gateway.run
```

启动日志会显示：
```
[admission] Controller initialized (db=..., audit=...)
[admission] Queue worker started
```

### 3. 查看队列状态

```bash
# 人类可读格式
python -m gateway.admission.cli status

# JSON 格式
python -m gateway.admission.cli status --json
```

### 4. 测试

发送 Feishu 消息，观察日志：
```
[admission] Admitted user=xxx lane=standard pos=1
[worker] Processing xxx from standard lane
[worker] Completed xxx in 2.34s
```

## 架构

```
Feishu Message
    │
    ▼
┌─────────────────────┐
│  AdmissionController │  ← 准入决策 + 分类
│  (controller.py)     │
└────────┬────────────┘
         │ enqueue
         ▼
┌─────────────────────┐
│   PriorityQueue      │  ← SQLite 持久化
│   (queue.py)         │
│                      │
│  ┌──────┐ ┌────────┐ ┌───────┐
│  │ fast │ │standard│ │ heavy │  ← 3 车道
│  └──┬───┘ └───┬────┘ └──┬────┘
└─────┼─────────┼─────────┼────┘
      │         │         │
      ▼         ▼         ▼
┌─────────────────────────────┐
│       QueueWorker            │  ← 3 并发 worker
│       (worker.py)            │
└──────────────────────────────┘
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

车道间并发：3 个 worker 同时从 3 个车道取任务。
车道内串行：同一车道内按优先级 FIFO 处理。

## 优先级

| 角色 | 优先级 | 说明 |
|------|--------|------|
| owner | 100 | 管理员，最高优先 |
| admin | 50 | 管理员 |
| member | 10 | 普通成员（默认） |

## 启用方式

在 `config.yaml` 中：

```yaml
platforms:
  feishu:
    extra:
      admission_control_enabled: true
```

默认关闭。开启后所有 Feishu 入站消息经过准入检查。

## 持久化

- 队列状态：SQLite (`~/.hermes/admission_queue.db`)
- 审计日志：JSONL (`~/.hermes/admission_audit/`)

重启后队列中未处理的消息会恢复。

## 文件结构

```
gateway/admission/
├── __init__.py          # 公开 API
├── types.py             # QueueItem, Lane 类型定义
├── queue.py             # SQLite 优先级队列
├── controller.py        # 准入决策 + 分类逻辑
├── worker.py            # 异步多车道 worker
├── persistence.py       # 持久化层
├── audit.py             # JSONL 审计日志
├── feishu_integration.py # FeishuAdapter 桥接
└── README.md            # 本文件
```

## 测试

```bash
pytest tests/gateway/test_admission*.py tests/gateway/test_queue*.py tests/gateway/test_audit.py tests/gateway/test_feishu_integration.py -v
```

当前 41/41 通过。

## 设计决策

1. **车道 vs 用户隔离**：当前按消息类型分车道，不按用户/群组隔离。原因：用户数不确定，动态创建 worker 复杂度高。如需用户级隔离，可在 standard 车道内加 per-user sub-queue。

2. **默认关闭**：通过 config flag 控制，避免影响现有单用户部署。

3. **错误降级**：准入检查失败时 fall-through 到原始处理流程，不阻塞消息。

4. **SQLite 而非 Redis**：单机部署场景，SQLite 足够且零依赖。分布式场景可替换为 Redis。

## 故障排查

### 队列卡住不处理

```bash
# 检查队列状态
python -m gateway.admission.cli status

# 检查 worker 是否启动
grep "Queue worker started" ~/.hermes/logs/gateway.log

# 清空队列（测试用）
python -m gateway.admission.cli clear
```

### 消息被拒绝

检查审计日志：
```bash
tail -f ~/.hermes/audit/$(date +%Y-%m-%d).jsonl | jq .
```

查找 `"result": "denied"` 的记录。

### 性能问题

查看处理时间：
```bash
grep "Completed.*in" ~/.hermes/logs/gateway.log | tail -20
```

如果某个车道处理时间过长，考虑：
- 调整车道分类逻辑（`controller.py` 中的 `_classify_lane`）
- 增加该车道的 worker 数量（需修改 `worker.py`）

## CLI 工具

```bash
# 查看状态
python -m gateway.admission.cli status [--json]

# 清空队列（仅测试用）
python -m gateway.admission.cli clear [fast|standard|heavy]
```
