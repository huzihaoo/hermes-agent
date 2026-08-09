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

1. 将非空归因摘要写入「归因结果」`field_9193cb`。
2. 将可访问的正式 HTML 报告链接写入「归因报告」`field_8c912e`。
3. 对两个字段分别执行 read-before、write、read-after。
4. 读回值与预期完全一致后，才把 delivery effect 标记为成功。
5. 写入结果不确定时只对当前 effect 做字段读回裁决，不盲目重发，也不启动历史全表 reconciliation。

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

## 7. 紧急止血（生产操作）

唯一对外写者是 delivery dispatcher。需要止血时必须按顺序卸载三个 launchd 服务：

```bash
launchctl bootout gui/$(id -u)/local.pnc.rca-delivery-dispatcher
launchctl bootout gui/$(id -u)/local.pnc.completion-notice-relay
launchctl bootout gui/$(id -u)/local.pnc.feishu-delivery-repair
```

使用 `bootout`，不要只 `kill`：这些 plist 具有 `KeepAlive`/`RunAtLoad`，单纯杀进程可能在 `ThrottleInterval` 后被重新拉起。`ExitTimeOut=30` 意味着已 claim 且仍在 lease 内的 effect 最多还可能完成；止血不是瞬时零风险。

止血后的只读确认：

```bash
for label in \
  local.pnc.rca-delivery-dispatcher \
  local.pnc.completion-notice-relay \
  local.pnc.feishu-delivery-repair; do
  launchctl print gui/$(id -u)/"$label" >/dev/null 2>&1 && echo "still-loaded:$label" || echo "unloaded:$label"
done
```

submission 熔断只阻止新任务进入，不能撤回已经存在的外发 effect；外发闸是 `HERMES_RCA_DELIVERY_DISPATCHER_ENABLED=false`，修改后需要重启/重新加载对应服务才生效。`HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK=false` 不是评论闸门，outbox 对 `true` 会 fail closed。

submission 熔断复位必须先做 plan，再由明确操作者以新 receipt 路径 apply；不能直接调用 store 方法或复用旧 receipt：

```bash
python3 scripts/pnc_rca_outbox_dispatcher.py \
  --clear-circuit \
  --operator '<operator-id>' \
  --reason '<bounded reason>' \
  --receipt '/absolute/path/rca-circuit-reset.json'

python3 scripts/pnc_rca_outbox_dispatcher.py \
  --clear-circuit \
  --operator '<operator-id>' \
  --reason '<bounded reason>' \
  --apply \
  --receipt '/absolute/path/rca-circuit-reset.json'
```

第一条只读并输出前态/计划后态；第二条在 SQLite 同一事务中写入 `control_meta` 审计记录并关闭 circuit，随后以 0444 receipt 和 `.sha256` sidecar 物化。receipt 或 sidecar 物化失败时，数据库状态可能已经关闭，命令会返回 `recovery_required`、`reset_id` 和 `meta_key`；先从该审计记录恢复 receipt，不得再次盲目 reset。

reset receipt 必须同时记录 active release identity、candidate env SHA 和当前 live env SHA；live env 漂移本身只作观察，不阻塞这个 operator-authorized、零 provider write 的 reset 命令。apply 仍必须匹配紧邻 plan 的 release binding、config、tool provenance、circuit 前态和 receipt 目标，真正字段写入继续由 dispatcher 的目标校验、幂等 marker 与 read-after-write 约束。

```bash
python3 scripts/pnc_rca_outbox_dispatcher.py \
  --materialize-reset '<reset-id>' \
  --receipt '/absolute/path/recovered-rca-circuit-reset.json'
```

`control_meta` 是事务内 create-once 的应用层审计记录，并非 SQL trigger 强制不可变；恢复命令会重新校验 schema、DB identity、canonical JSON 和 fingerprint。`effect_delta.external_writes=0` 仅声明这个 reset 命令本身没有外部写路径，不能替代 B15 的 delivery DB 前后计数证据。

B15 最终只读 preflight 必须把 resident/config、历史 outbox hold、Kafka freeze、120 秒 resource snapshot、record-only 完整配置、delivery disabled 和 effect baseline 收敛到一张 receipt；任何一项缺失都返回 `RED`，不启动或重载服务：

```bash
python3 scripts/pnc_rca_b15_preflight.py \
  --env-file "$HOME/.hermes/.env" \
  --runtime-dir "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca" \
  --launch-agents-dir "$HOME/Library/LaunchAgents" \
  --control-db "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3" \
  --resource-snapshot '/absolute/path/resource-snapshot.json' \
  --receipt '/absolute/path/b15-preflight.json'
```

该脚本只读打开 SQLite（`query_only`），receipt 另写入 0444 文件和 hash sidecar；`RED` receipt 也必须保留，不能把空输入、非法嵌套 snapshot 或缺失 snapshot 当成绿灯。CLI 只使用执行时的 live UTC clock，不提供 `--now` 或其他 freshness 覆盖参数。resident gate 会校验四类 health 的 exact schema、关键布尔/config、各自权威 freshness 字段、完整 runtime identity 及 plist/release 绑定；历史 outbox gate 会用 v13 canonical table/trigger validator 重算 sealed/current 非空 cohort，并要求 Kafka freeze epoch 与当前 activation epoch 一致。`HERMES_RCA_OUTBOX_ALLOW_FEISHU_WRITEBACK=false` 也必须显式存在，缺失不能继承默认值。`external_effect_baseline` 是后续零增量比较的起点，不等同于已经完成零增量验收。

恢复前先取 service PID、配置指纹、数据库 effect 增量和 receipt；不得用 `kill -9`、跳过 `bootout`，或在未记录 receipt 时直接重新加载 plist。上线前演练应在隔离/无写环境完成，并记录最多 30 秒的残余 lease 窗口。

## 8. 故障定位

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

## 9. 清理与保留

迁移 worktree、candidate runtime、precutover 快照、stage plan/lock、发布评测器和退役 gate 应删除。只保留：

- 当前主仓与 `hermes-live`
- 当前生产 identity/binding 的一份 canonical receipt
- 当前 RCA workspace manifest
- 控制库和历史不确定投递审计
- 真实业务任务的必要报告与阶段 receipt

历史不确定投递记录不能作为普通缓存删除；必须在字段 reconciliation 完成后按审计规则处置。

## 10. v19 最小稳定生产路径

本节定义 v19 能力上线后处理真实问题单的最短路径；只核验本次变更面和当前批次，不恢复旧 release gate、历史对账或合成入口验收。

1. **绑定 v19 runtime。** Host 的 VM release binding、RCA workspace 的 fixed CLI、VM worker 的 `RCA_SERVICE_REPO_ROOT` 与任务状态中的 `route/cwd/fixed_cli_entrypoint` 必须共同指向当前 installed runtime。本轮基准路径是 `/home/mini/.hermes/rca-prod-runtime/releases/rca-platform-20260809.installed-eeb1bb9`；后续切换时用新的完整 installed 路径整体替换，不混用两个 runtime。
2. **补齐 remote-reader 生效面。** installed runtime 的 Git commit/tree clean 不代表 remote reader 已就绪；`.rca-runtime` 是 Git ignored 的本地生效面。先在目标 runtime 执行 `api/g1q3_rca/scripts/bootstrap_remote_reader_runtime.py --check`；若返回 `remote_reader_runtime_not_bootstrapped`，执行同一脚本的 `--install-offline`，再独立 `--check` 到 `status=ready`。不得联网补包，也不得用 Host 预扫描替代 VM 检查。
3. **核对三面 identity。** Host 核对 source commit/tree、clean 状态和 VM binding；Workspace 核对 manifest SHA、closure SHA 与 fixed CLI；VM 核对 pipeline commit/tree、正式 origin、必需 submodule、installed path，以及 worker 实际 route/cwd/fixed CLI。任一面不一致只修该面，未变化的面不重复发布。
4. **直接交付并读回。** 对真实 issue 只写 `field_9193cb` 和 `field_8c912e`，每个字段执行 read-before、write、read-after；两项官方回读与本代 payload 完全一致后才记成功。评论、报告文件存在或本地 receipt 都不能替代字段读回。
5. **历史 delivery 不阻塞新批。** 批量处理只以当前 generation 的可调度工作和实时资源为背压依据；旧 generation 的 pending/stalled/uncertain/terminal 行保持审计态，不得倒灌成新批 admission blocker。当前 delivery 水位配置为 `HERMES_RCA_OUTBOX_DELIVERY_HIGH_WATERMARK=10000`、`HERMES_RCA_OUTBOX_DELIVERY_RESUME_WATERMARK=9999`，它不同于 Kafka outbox 水位，用于避免历史 delivery backlog 把新问题单入口压停；不得为清旧账而降低水位或暂停当前批次。
6. **失败代次只前进，不复活。** 已失败或终止的 task、submission key 和 delivery effect 保持不可变。需要重试时必须显式创建 `generation + 1`，生成新的 submission key/task ID，并绑定当前 runtime identity；禁止把旧 task 改回 pending、复用旧任务目录或重新 claim 旧 effect。
7. **不模拟机器人入口。** operator issue-only 批次不创建 `@机器人`、话题回复或卡片更新，也不把这些动作作为字段写入验收门。真实 `@` 入口仍沿用原 `chat_id/thread_id/message_id` 和既有 topic/card 链路，由真实事件触发并按原功能回归；发布与批处理不得合成事件替代它。
8. **VM 排队时间不计入执行超时。** Host collector 对 `pending/submitted/queued/claimed/running/in_progress` 只轮询，不得从 outbox 完成时间或入队时间生成执行 deadline；真实执行超时由 VM worker 从进程启动时起算并产出终态。Host 的 30 分钟失败回退只从 `terminal_first_seen_at` 的首次真实失败观测起算，后续健康状态必须清空该窗口。有效 completed 产物无论排队多久都进入同一套产物校验和字段交付。

最小验收结果是一张当前批次清单：`work_item_id`、generation、Host/Workspace/VM identity、两个字段的 read-after 值和最终状态。旧 baseline 重放、全历史差异矩阵、历史 delivery 清算及额外 canary 均不属于该路径。
