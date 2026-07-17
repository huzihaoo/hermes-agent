# G1Q3-RCA 生产功能 Runbook

> 适用范围：Hermes v0.18.2 已上线后的 RCA 业务开发、故障处理和端到端验收。
> 当前状态以 live runtime、控制库、服务 health、Kafka broker 和飞书问题单读回为准。
> 不再使用 14.6 -> 18.2 迁移 worktree、candidate runtime、release gate、容量评测、24 小时 soak 或 7 天样本门禁。

## 1. 唯一代码与运行路径

| 用途 | 路径 |
|---|---|
| 开发主仓 | `/Users/songying/.hermes/hermes-agent` |
| 生产运行代码 | `/Users/songying/.hermes/runtime/hermes-live` |
| RCA workspace manifest | `/Users/songying/.hermes/runtime/rca-workspace-runtime` |
| 控制与投递库 | `~/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3` |
| VM 业务仓 | `/home/mini/data3/yj-evaluation-server` |
| VM 任务产物 | `/mnt/tmp/<submission_key>/` |

代码只在开发主仓修改并提交。生产生效时只同步本次业务变更涉及的文件，校验 SHA 后重启受影响的 resident；不重建 Hermes runtime，不创建发布沙箱或候选 worktree。

## 2. 业务链路

```text
Kafka feishu-project-workflow-event
  -> 固定 group rca_root_cause_analysis_agent
  -> 读取真实飞书问题单
  -> 识别功能域和 DNP 关键字
  -> 解析问题发生时间并定位前视 frame_id
  -> durable outbox
  -> VM S2/S3a/S3b/S5/S6
  -> delivery collector
  -> 写入「归因结果」「归因报告」
  -> 逐字段读回确认
  -> 输出已处理问题单清单供人工 review
```

生产必须满足以下约束：

- Kafka 使用真实 topic、固定 group 和已授权的 `rca` principal；不得用影子 group 的成功替代生产消费。
- 最近 7 天 Kafka 消息只用于定位真实问题单和功能验收；探针必须禁用 auto commit，不改生产 group offset。
- 问题数据只走 PDCL remote read，不恢复 MDI 下载或本机输入物化。
- VM 使用 fixed direct CLI，`agent_backend=none`，不允许 Agent fallback。
- 归因输出保持 `need_review`，由业务人员在问题单上确认。
- 成功交付必须写指定字段并读回；评论或话题回复不能替代字段写入。

## 3. 业务字段规则

### 3.1 功能域与 DNP

DNP 从问题名称、问题所属部门和简洁部门字段中识别。至少支持：

- `规划`
- `SPP`
- `OOI`

中文使用规范化后的子串匹配；ASCII 关键字使用字母数字 token 边界匹配。ACC、LCC、AEB、FCW 等继续使用已确认的功能域映射。

### 3.2 问题时间与 frame_id

「问题发生 frame_id」允许直接填写正整数，也允许填写测试人员打点时间，例如：

```text
2026-07-12 15:31:16
20260708, 20:05:00
```

时间处理顺序：

1. 按 `Asia/Shanghai` 解析问题单时间。
2. 转为管理面 Unix 微秒时间戳。
3. 在固定前视相机 topic 中查找对应帧。
4. 秒级打点允许 1 秒内最近帧；同差值选择更早帧。
5. 超出容差、无前视数据或结果不唯一时 fail closed，不猜 frame_id。

### 3.3 飞书结果字段

S6 成功后必须完成：

1. 将非空归因摘要写入「归因结果」。
2. 将可访问的正式 HTML 报告链接写入「归因报告」。
3. 对两个字段分别执行 read-before、write、read-after。
4. 读回值与预期完全一致后，才把 delivery effect 标记为成功。
5. 写入结果不确定时进入 reconciliation，不盲目重发。

字段 ID 必须从当前飞书字段元数据解析并与字段名称核对，禁止依赖历史手工常量。

## 4. 端到端验收

一次有效的功能验收必须绑定同一个真实 `work_item_id`，并留下以下证据：

| 阶段 | 必须证明 |
|---|---|
| Kafka | topic、partition、offset、work item identity；生产 group 消费可见 |
| 飞书读取 | 问题名称、功能域来源、远程数据引用、问题时间可解析 |
| DNP | 命中的关键字和最终映射，原始敏感字段不进入通用 receipt |
| frame | 管理面时间戳、相机 topic、匹配时间戳、frame_id、delta |
| S2 | 完整远程读取、扫描范围、派生 MCAP seal |
| S3b | 转换返回码为 0、输出 size/SHA/MCAP seal |
| S5/S6 | 对齐完成、`report_data.json`、`index.html` |
| 飞书交付 | 两个字段写入与 read-after-write 一致 |
| Review 清单 | work item ID、归因摘要、报告链接、交付状态 |

验收只针对需求链路跑定向测试和一个真实问题单。不得为了“发布完整性”重新运行已退役的全量回归、release gate、容量、soak 或历史 replay。

## 5. 日常健康检查

```bash
# Host
curl -fsS http://127.0.0.1:18789/health/detailed | python3 -m json.tool
meegle auth status --format json
for name in health outbox_dispatcher_health delivery_collector_health delivery_dispatcher_health; do
  test ! -e "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/$name.json" || \
    python3 -m json.tool "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/$name.json"
done

# VM，只读检查
~/.local/bin/ssh-mini-status
~/.local/bin/ssh-mini-resource --summary
~/.local/bin/ssh-mini-mcap-status --summary
```

生产常驻服务：

- `local.pnc.rca-kafka-consumer`
- `local.pnc.rca-outbox-dispatcher`
- `local.pnc.rca-delivery-collector`
- `local.pnc.rca-delivery-dispatcher`
- `local.pnc.vm-task-sync`

dispatcher 可以在字段协议修复期间保持停止或 circuit open；这不是 Kafka 鉴权失败。恢复写入前必须先用一个真实问题单完成字段写入与读回。

## 6. 定向测试

根据实际改动选择最小测试集：

```bash
cd /Users/songying/.hermes/hermes-agent
RCA_PYTHON=/Users/songying/.hermes/runtime/hermes-live/.venv/bin/python
PYTHONDONTWRITEBYTECODE=1 "$RCA_PYTHON" -m pytest -q -o addopts='' \
  tests/gateway/test_pnc_rca_frame_reference.py \
  tests/scripts/test_pnc_rca_kafka_consumer.py \
  tests/scripts/test_pnc_rca_outbox_dispatcher.py \
  tests/scripts/test_pnc_rca_delivery_collector.py \
  tests/scripts/test_pnc_rca_delivery_dispatcher.py
```

不要默认运行整个 Hermes 测试集。VM 改动只运行对应 pipeline/service 测试和同一真实任务产物的阶段级验证，不新建无业务价值的 generation。

## 7. 故障定位

| 信号 | 处理 |
|---|---|
| Kafka consumer 未运行或无新 offset | 查 resident health、broker 连接、固定 group 和 topic；认证通过后不要改 group 名绕过问题 |
| `host_issue_preread_failed` | 查 Meegle/Feishu 读取链路和网络；飞书权限已验证时不要误报为长期权限 blocker |
| `missing_frame_id` | 核对时间格式、时区、前视 topic 和 1 秒容差；不扩大到无业务依据的宽窗口 |
| `remote_scope_incomplete` | 完整读取请求范围；不得静默截断消息数 |
| `translate_failed` | 查任务专属 Docker 输出、设备和镜像；不要求不存在的 NVIDIA runtime |
| `alignment_failed` | 查 S5 参数、case root 和 task root；优先复用同一任务已完成的 S2-S45 产物 |
| 报告存在但字段为空 | 修 delivery adapter 和字段元数据映射；不要用评论冒充交付 |
| 远端写入结果不确定 | 保持 circuit open，按 marker/UUID/字段读回裁决，不直接重试 |

## 8. 清理与保留

迁移 worktree、candidate runtime、precutover 快照、stage plan/lock、发布评测器和退役 gate 应删除。只保留：

- 当前主仓与 `hermes-live`
- 当前生产 identity/binding 的一份 canonical receipt
- 当前 RCA workspace manifest
- 控制库和历史不确定投递审计
- 真实业务任务的必要报告与阶段 receipt

历史不确定投递记录不能作为普通缓存删除；必须在字段 reconciliation 完成后按审计规则处置。
