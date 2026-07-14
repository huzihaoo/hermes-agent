# G1Q3-RCA 运维与接力 Runbook

> 面向：接手这条业务线的下一个 agent / operator。
> 真相边界：本文是操作手册和现场快照（生产链路最后修订 2026-07-12）。当前状态一律以 live runtime、release-gate receipt 和已部署 clean commit 为准。
>
> **强制边界（2026-07-11）**：问题创建可由 Kafka workflow event 自动触发，或由固定业务群 active subset 内真实 `@小助手 + 明确动作 + 唯一 canonical issue identity` 人工触发；两者必须进入同一 durable outbox/固定 service。普通用户的分析/紧急分析只能 `run_or_join`，`rerun/debug` 只对受控 operator 开放。禁止 MDI 下载、`allow_download=true`、旧群聊直提 VM、公共 `vm_task_submit`、Agent fallback 和手工 `run_rca_auto_pipeline.py` 生产提交。下文提到的下载时代结果仅是历史证据，不是可执行操作。
>
> 配套文档：
> - 业务契约与状态机：`docs/pnc/g1q3-rca-intake-state-machine.md`
> - 当前全链路设计：`docs/pnc/g1q3-rca-auto-pipeline-design.md` 的“当前有效设计”；其下载时代附录不作为进度或操作依据
> - 接手快照索引：`outputs/pnc/g1q3-rca-agent-handoff-20260611.md`
> - **Codex 接力任务书（2026-06-12 整轮汇总）**：`outputs/pnc/g1q3-rca-round-handoff-20260612.md`（T1-T4 可直接执行；资产登记表）
> - 根因语料归档：`outputs/pnc/g1q3-rca-corpus-20260612/`（1000 单扫描 + 267 单评论）

---

## 1. 系统全景（一张图）

```
飞书问题单创建事件 → Kafka feishu-project-workflow-event ─┐
受控业务群 @ + 明确动作 + 唯一 issue identity ───────────────┤
                                                               ▼
  │ host: source-neutral admission → durable outbox dispatcher
  │   ├─ 固定 consumer identity / creation policy / offset fencing
  │   ├─ Feishu API 读取并固化 issue 字段
  │   ├─ remote-read v2 request + derived-capacity atomic reservation
  │   └─ vm_task_submit_service（capability-scoped, create-once）
  │
  ▼ VM: hermes-vm-coding-worker-daemon + lane scheduler（live slots 以 fresh health 为准）
  fixed direct CLI（agent_backend=none, fallback=false）
    S1 门禁 → S2 pdcl_pyclip远程读 → S3a派生流 → S3b转换 → S5对齐 → S6归因+报告
    receipt: <output>/pipeline_state.json（断点续跑）
  │
  ▼ 产物: /mnt/tmp/<submission_key>/ 隔离目录（index.html + report_data.json，归因永远 need_review）
  ▼ 回传: delivery collector → required issue/topic effects → delivery dispatcher → 问题单 + 原任务话题
```

关键路径契约：
- Kafka project key/simple name、work-item type 和 creation transition 必须来自已审定真实 fixture + versioned allowlist，不从历史常量猜测。
- 任务根：`/mnt/tmp/<submission_key>/`；用户可见 CIFS：`//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<submission_key>/`。
- HTML 只能使用 delivery manifest/contract 已证明可读、依赖闭合且 browser smoke 通过的 URL；不得从旧 cases root 或文件存在性推导交付链接。
- Host、worker、VM 业务源码都必须是 release BOM 绑定的 clean candidate commit；不直接在 live repo 开发或测试。
- MCAP 转换必须经 task-owned governed wrapper，镜像 pin digest，显式 memory/CPU/PID/timeout、network none 和退出清理；禁止裸跑 Docker 或 `mcap_service`。

## 2. 日常健康检查（30 秒）

```bash
# Host（服务部署后；health 必须携带当前 runtime identity）
curl -fsS http://127.0.0.1:18789/health/detailed | python3 -m json.tool
meegle auth status --format json
for name in health outbox_dispatcher_health delivery_collector_health delivery_dispatcher_health; do
  python3 -m json.tool "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/$name.json"
done

# VM（只读；遵守 /Users/songying/AGENTS.md）
~/.local/bin/ssh-mini-status
~/.local/bin/ssh-mini-resource --summary
~/.local/bin/ssh-mini-mcap-status --summary
```

## 3. 开关与生产边界（改常驻服务配置后需按生效链重启）

| 环境变量 | 默认 | 作用 |
|---|---|---|
| `HERMES_RCA_KAFKA_SUBMIT_ENABLED` | **false** | safe-off 默认关闭。最终候选配置在 preauthorization 前冻结为 true。物化原子窗口五个 writer 全停；随后可恢复 exact candidate Gateway 服务其他能力，但四个 RCA resident 仍停，且 Gateway 在无 epoch/`safe_off` 下必须拒绝 RCA admission。禁止再用 false 生成无 activation binding 的 shadow canary。 |
| `HERMES_RCA_KAFKA_ACTIVATION_REQUIRED` / `HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED` | **false** | activation 发布窗口必须均为字面量 `true`；只接受 `true/false`，不接受 `1/yes/on`。 |
| `HERMES_RCA_ACTIVATION_REQUIRED` | **false** | Gateway 群 `@` 入口的同一 activation 绑定；preproduction 至 steady 全程必须为字面量 `true`。 |
| `HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED` / `HERMES_RCA_DELIVERY_DISPATCHER_ACTIVATION_REQUIRED` | **false** | collector 与 Feishu effect 写边界必须绑定同一 current epoch；发布窗口均为字面量 `true`。 |
| `HERMES_RCA_MANUAL_INTAKE_ENABLED` | **false** | 群 @ 人工入口独立总闸；只在 mention/授权 receipt、双入口竞态、原话题交付 canary 全绿后开启。 |
| `HERMES_RCA_MANUAL_CHAT_IDS` | **空** | 人工入口 active subset；只接受固定生产/测试群 ID 的非空子集。测试群资格可在 final epoch 前演练；最终 epoch 的 config/build fingerprint 必须冻结包含正式群的最终集合，并在同一 bounded Gateway PID 上重做正式 success/failure canary。bounded 后不得改集合或重启。 |
| `HERMES_RCA_MANUAL_OPERATOR_ENABLED` / `HERMES_RCA_MANUAL_OPERATOR_USER_IDS` | **false** / 空 | `rerun/debug` 共用的 operator 总闸与用户 allowlist；普通用户仍可 `run_or_join`。production 开启 manual intake 时必须显式设置 canonical operator 开关，即使值为 false；若为 true，canonical 用户名单必须显式非空。`HERMES_RCA_MANUAL_DEBUG_ENABLED` / `HERMES_RCA_MANUAL_DEBUG_USER_IDS` 只作 canonical 值缺失时的兼容 fallback，不得作为新部署主配置。 |
| `HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT` / `HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS` | **3** / **600** | 每 requester 的 durable 事务限速；production operator 开启时必须显式设置为正整数 `3` / `600`，非法、空值或依赖运行时默认均阻断发布，不能靠重启绕过。 |
| `HERMES_RCA_OUTBOX_DISPATCH_ENABLED` | **false** | VM 创建总闸门；与 submit 分开，回滚优先关闭此项，再关闭 submit。 |
| `HERMES_RCA_PROD_CAPACITY_MODE` | **steady** | 首次生产 release 候选固定为 `bootstrap`；bootstrap 不降低 `rca_prod` 资源类，只使用独立、最多 8 天、并发 1、每日 5 次的 owner 授权。不得用 bypass 或改成 `standard/vm_heavy`。 |
| `HERMES_RCA_PROD_RELEASE_ID` / `HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID` | **空** | bootstrap 时必须在计算 BOM 前成对冻结，并进入 resident runtime config fingerprint。BOM、approval SHA、authorization SHA 不得写入 BOM 覆盖的 env/plist，避免哈希自引用。 |
| `HERMES_RCA_PROD_ADMISSION_HMAC_KEY` | **空** | Host-only admission receipt 签名密钥，至少 32 decoded bytes，仅接受 `hex:`/`base64:`；不得复用 Kafka/Feishu 密码，也不得进入 health、plan 或 receipt。 |
| `HERMES_RCA_DELIVERY_COLLECTOR_ENABLED` / `HERMES_RCA_DELIVERY_DISPATCHER_ENABLED` | **false** | 交付读取和 Feishu effect 派发分别启用；必须先证明 read-after-write、幂等和 backlog/circuit。 |
| `HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS` | **110** | 读取最多 512 MiB 的 sealed delivery bundle；最大 110 秒，且 collector lease 必须至少多 15 秒。延长 deadline 不放宽文件数、单文件、总字节、路径或 hash 上限。 |
| delivery effect lease keeper | **强制开启，10 秒** | 无关闭开关；release gate 要求 `effect_lease_keeper_enabled=true`、续租周期不超过 15 秒且不超过 lease margin。health 必须公开 active/started/stopped/renewals/failures；任一续租或 fence 失败禁止旧 worker 提交本地成功态。 |
| `HERMES_G1Q3_AUTO_DOWNLOAD_DAILY_QUOTA` | **必须为 0** | 下载时代兼容变量；当前生产路径不读取它，release gate 非 0 即阻塞。不得用它恢复下载。 |
| `HERMES_RCA_LEGACY_AUTO_EXECUTION_DISABLED` | **必须为 true** | 旧群聊直连 VM/Agent 路径永久关闭；新的人工入口只能走 source-neutral durable outbox。 |
| `G1Q3_GOVERNANCE_DOWNLOAD_ENABLED` | **必须为 false** | 旧 governance 下载协调器已退役；代码边界不可重新开启。 |
| `HERMES_G1Q3_ISSUE_CAPTURE_ENABLED` | **必须为 false** | issue preread 默认无写副作用；诊断 capture 只能显式启用并落 `/mnt/tmp/<task>/`，生产 release gate 禁止启用或配置 root。 |
| `HERMES_G1Q3_FIELD_GAP_COMMENT` / `HERMES_G1Q3_REPORT_COMMENT` | 未设（关） | 旧直写路径保持关闭；新链路只允许 durable delivery effect。不得与 delivery dispatcher 双写。 |
| `HERMES_G1Q3_MCP_AUTODEGRADE` | 1（开）| Meegle 失效自动降级 MCP；=0 关闭。整段 issue preread 总预算 75 秒，Meegle 单次 12 秒、MCP 单次 15 秒；超时只进入 durable retry。 |
| `HERMES_G1Q3_MCP_FALLBACK` | 未设 | 显式强制启用 MCP fallback（诊断用）|

旧下载额度 ledger 只供历史审计；当前容量真相是 derived-capacity reservation/usage receipt。字段评论仍使用独立 ledger。

首次放量的 bootstrap authority 只允许固定文件 `~/.ssh-mini/rca-bootstrap-capacity-authorization.json`：regular、owner-only `0600`、单链接、非 symlink，且每次 health、claim 和 submit 都重新读取。release prepare 先生成 BOM/主审批，随后才可签发该 authority。production env stage 的 receipt 是 post-BOM `active-release-binding`；cutover 必须把 candidate `.env` 与该 receipt 原子安装为 live `.env` 和 `<runtime_state_root>/active-release-binding.json`，并把两者纳入同一 snapshot/journal/rollback。常驻 dispatcher 会交叉校验 live env SHA、静态 release/epoch、BOM、主审批 raw SHA、authority raw SHA/fingerprint；仅替换 authority 或 binding 任一侧都会 fail closed。

bootstrap 授权最长 8 天，只用于收集 steady capacity 所需的至少 20 个、跨度至少 7 天且 `input_materialized_bytes=0` 的真实成功样本。最终 bootstrap candidate 必须在 preauthorization 前冻结 `HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED=true`；进入 `bounded_active` 后、启动四 resident 前，由 owner 执行 `prepare-bootstrap-production` 发布 create-once producer receipt。receipt 的历史 `activated_at` 到授权 deadline 必须至少剩余 7 天 6 小时；少 1 微秒也拒绝，避免 generation 1 create-once latch 落入永远不能形成 7 天 ledger 的窗口。已有 receipt 重试、`production_bootstrap` gate 和业务 steady apply 时，当前时间到 deadline 仍必须至少剩余 7 天。collector 只消费该 receipt，绝不自行创建。禁止在授权到期前临时手改 env、重启绕过或延长同一 epoch。

业务 Activation `steady_active` 与容量 `STEADY_ACTIVE` generation 2 不同：前者开放 Kafka/群 @ 生产入口，但继续受 bootstrap 并发 1、每日 5 次限制；后者仅在 20 个/7 天样本和五件证据闭合后不可逆放大容量，不再次修改业务 Activation。容量 evidence root 固定为 control DB 同目录的 `rca-capacity-transition/`，必须同时保留 `steady-intent.json`、`steady-authorization.json`、`steady-receipt.json`、`steady-marker.json`、`evidence-bundle.json`；任何 prefix 缺失都保持 bootstrap 或 fail closed。

共享 outbox 对人工新执行采用 80% active 水位上限，为 Kafka 自动建单保留容量；同一问题的 `run_or_join` 仍可 join 已有 generation。claim 顺序是持久化的有界公平调度：最多连续 3 个 Kafka 后，若有人工任务等待则处理 1 个；不得改回纯 FIFO 或单一来源永久优先。Outbox 外部边界执行期间每 10 秒独立刷新 liveness；delivery effect keeper 每 10 秒续租、最大允许间隔 15 秒。服务 heartbeat 只证明进程存活，keeper 只证明 claim 仍受当前 fence 保护，两者都不能覆盖 error/circuit/readiness。发布 gate 仍要求 readiness observation 新鲜，长任务结束后必须完成一次完整 health refresh。

Feishu Lark client 和话题每次读写均为 12 秒 deadline。读超时进入 durable retry；写超时必须标为 outcome uncertain，并使用既有 marker/UUID reconciliation，禁止直接盲重发。`asyncio.to_thread` 的超时不能终止底层 OS 线程，因此 keeper 只保证失租 worker 不落本地成功；任何晚到远端结果仍必须由 marker/UUID 读回裁决。delivery collector 的 110 秒 bundle deadline 与单 case 120 秒 remote-read 总边界是两个不同阶段，均不得回退 MDI。

安装候选 plist、修改 live `.env`、启动或重启 gateway/Kafka/outbox/delivery/VM worker 都属于生产生效动作，只能在 release gate 全绿并获得明确发布批准后，按 `pnc-business-prod-effect-chain.md` 执行。代码合入或本地测试不等于生效。

`scripts/pnc_rca_production_cutover.py` 的 CLI 只提供 read-only `plan/validate`；不存在 CLI apply、默认 system adapter 或 shell runner。任何程序化 apply 都必须显式注入全局 lease、release-gate validator 和另行审核的 system adapter，并使用 owner-only no-clobber journal、live CAS、逐步授权、物理 payload SHA、rollback intent/done。该 adapter 未单独批准前，不能把 fake-only tests 解读为生产切换授权。

Control store v10 与 delivery store v6 是当前 forward-only schema。上线必须按 configured DB 的 live 事实选择一条且仅一条路线：已有 exact v8/v5 时，先停写并做一致性备份/恢复，且由同 commit/BOM、`100755`、单链接的 predecessor validator 以真实 subprocess 只读验证；validator 缺失即 NO-GO。configured DB 真实缺失时走 greenfield：migration v3 只生成 seed 并声明 materialization blocker，显式 materializer 再以 maintenance marker、prepared/installed/receipted journal 和 genesis receipt 原子建立 v10/v6；不存在一个可供旧 binary 恢复的历史 live DB。`already_current` 与 DB 内六项 genesis/origin meta 只表示内部连续性，不能自证可信来源或作为 rollback evidence；后续发布必须继续消费并完整重验原始 materialization receipt/intent/journal，缺失即 NO-GO。

所有生产入口只 open-existing：Kafka、Outbox、delivery collector/dispatcher、Gateway manual admission 和 activation CLI 均要求绝对、regular、单链接、非空且精确 v10/v6 的 SQLite；缺库/旧 schema 不自动创建或迁移。`<db>.pnc-rca-maintenance` 与 `<db>.pnc-rca-tombstone` 在启动和每次 connect/write 前均 fail closed，禁止手删 marker 绕过 journal recovery。resident health 必须分别精确报告 `pnc_rca_control_store_v10` / `pnc_rca_delivery_store_v6`。

Activation 是唯一生产放量控制面，不允许把 legacy `submit=false` shadow 当成 canary。固定顺序如下，进入 bounded 后到 steady 之间不得重启四个 RCA resident 或 Gateway：

1. 在单一发布窗口停止四个 RCA resident 与 Gateway，生成五项 writer-stop receipt，并完成 predecessor drill 或 greenfield materialization。该全停窗口只覆盖原子 Store 操作。
2. 物化后可立即启动 exact candidate Gateway 以恢复其他机器人能力；四个 RCA resident 保持停止。Gateway 必须 activation-required，且在无 epoch时拒绝 RCA admission、无 pending/active effect，PID/config/build 自此冻结。
3. 运行 preauthorization gate；成功后必须看到 receipt、`activation-preauthorization.json` 和 `activation-preauthorization.commit.json` 三件套，且只使用已由 marker 提交的 capsule 创建 `safe_off` epoch。operator 不得自行填写 fingerprint、DB identity 或 start fence。
4. 在四 resident 仍停止、Gateway 仍被 `safe_off` 持有时运行 preproduction gate，重验 preauthorization/config/typed DB identity/T0/冻结 canary plan/迁移或物化连续性；成功同样要求 receipt、capsule、commit marker 三件套，只使用已提交 capsule 执行 `safe_off -> preauthorized`。
5. 使用同一 preproduction capsule 逐条授权 `kafka_success`、`manual_success`、`manual_terminal_failure` 三个 exact identity；manual identity 必须包含原任务 `thread_id`。`transition-bounded` 再按 capsule 逐槽复核 Store authorization 后才转 `bounded_active`。
6. bounded 后先由 owner 对 `prepare-bootstrap-production` 执行同参数 plan/apply，绑定同一 preproduction capsule、active-release binding、live env、release/epoch 和 fixed bootstrap authority，create-once 发布 producer receipt；receipt 未就绪时禁止启动 resident。
7. producer receipt 就绪后一次性启动四个 RCA resident，不重启 Gateway；三个槽位各消费一次，execution 必须真实绑定 trigger/outbox/ledger 并形成完成证据。
8. readiness 变为 ready 后 consumer 自动暂停全部 assignment。broker group offset 非负时必须等于 freeze position；missing/`-1` 只在 freeze position 等于 owner T0 时允许并回到 T0，绝不能伪造 commit。production gate 将每分区 `source/offset/broker_group_offset/freeze_position/start_offset`、三 canary、final writer barrier 和 release receipt 绑定进 production fingerprint。
9. production gate 成功后原子发布 receipt、confirmation capsule、confirmation commit marker 三件套；只有 marker `publication_complete=true` 且精确绑定前两者才可 `confirm`。`confirm` 以 versioned exact-shape receipt 和当前 health 重算 release binding，并执行 runtime round 1 -> Kafka freeze/freshness 回读 -> runtime round 2。`confirmed` 继续冻结且关闭 claim，只允许逐事件 reconcile 当前 epoch `[start,end)` 内的 bound shadow；fence 外 offset 不落库、不提交，steady 后由同一 consumer 无重启重试。
10. current/global shadow 清零后才转业务 `steady_active`。任何 pending inbox、claimed writer、未绑定 ledger、历史未处置 shadow、配置/PID 重启或 end-fence SHA 漂移都阻断状态转换。此时容量仍是 generation 1 bootstrap，直到 20 个/7 天样本和容量 executor 的 generation 2 CAS 独立完成。

运维只使用 `scripts/pnc_rca_activation.py`。`status` 只读；所有 mutation 默认只输出脱敏 plan，必须显式 `--apply`、exact epoch/event、operator 和 reason。紧急 `abort` 先阻断新 claim；尚未执行的 current bound shadow/pending 必须逐事件执行 `defer-event`，形成 quarantined disposition 与不可变 audit。全局仍有 shadow 时 control store 拒绝创建替代 epoch；`deferred_quarantined` 必须进入人工重触发/backfill 清单，不能视为已完成分析。

Feishu 原任务话题交付依赖 delivery dispatcher 常驻进程的**精确 launchd 解释器**，不是当前 shell 中碰巧可用的 Python。candidate plist 与安装后 plist 都必须从 `ProgramArguments[0]` 取解释器执行下列探针，并将 plist SHA、解释器路径/realpath、SDK 版本和 API 结果写入 release receipt；任何一步失败均 production NO-GO：

```bash
RCA_DELIVERY_PLIST="${RCA_DELIVERY_PLIST:?set candidate or installed delivery dispatcher plist}"
RCA_DELIVERY_PY="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$RCA_DELIVERY_PLIST")"
"$RCA_DELIVERY_PY" -I - <<'PY'
from importlib import metadata
from lark_oapi.api.im.v1 import ReplyMessageRequest, ReplyMessageRequestBody

assert metadata.version("lark-oapi") == "1.5.3"
assert callable(getattr(ReplyMessageRequest.builder(), "message_id", None))
assert callable(getattr(ReplyMessageRequestBody.builder(), "reply_in_thread", None))
print("lark-oapi topic-reply dependency: ok")
PY
```

## 4. 故障分类与恢复（按 blocker kind 查表）

**原则：三类问题不得互串——主控读取失败 ≠ 业务字段缺失 ≠ 管线 stage 失败。**

### 4.1 读取层（host）
| 信号 | 含义 | 处置 |
|---|---|---|
| `host_meegle_preread_unauthenticated` / 群里"请重新授权 Meegle" / home channel 告警 | Meegle token 过期（约 2h 寿命，无自动续期）| `meegle auth login --device-code --host project.feishu.cn`。期间 MCP 自动兜底，业务通常不断 |
| `host_issue_preread_failed`（双链路都挂）| Meegle 和 MCP 都不可用 | 查网络 / MCP server / `meegle auth status`；偶发空读重试一次即可（已知 Meegle 偶发瞬时空读）|

### 4.2 字段层（业务方动作，不是技术故障）
| 信号 | 处置 |
|---|---|
| `issue_field_missing_remote_data_reference` / `issue_field_invalid_remote_data_reference` | 等 owner 在卡片补明确的 event UUID 或 clip UUID；系统只把历史命令形状解析为地址，不执行命令。 |
| `missing_frame_id`（VM 门禁）| 等 owner 补「问题发生frameid」；不要替用户从描述里猜帧号 |
| `unsupported_function_domain` | 域不在 ACC/LCC/HMI/AEB/AWB/FCW/FCTB/DNP/ELK 内；扩域改 VM `check_case_gate.py` 的 `SUPPORTED_DOMAIN_ALIASES`（需确认有对应 evaluator）|

### 4.3 管线层（VM，看 `/mnt/tmp/<task>/pipeline_state.json`）
| blocker | 处置 |
|---|---|
| `remote_read_dependency_unavailable` / `remote_read_timeout` | 检查固定 `/usr/bin/python3` sidecar、`pdcl_pyclip==0.1.6+rca.2` 指纹、远端服务与 120s timeout；不得回退 MDI。 |
| `remote_scope_incomplete` / `remote_read_limit_exceeded` | 请求范围未完整证明或超过消息/扫描/输出上限；拆分输入或人工复核，不得静默截断。 |
| `derived_capacity_*` | 检查 storage admission v2 对 `/mnt/tmp` 的单次 statvfs、`input_materialization=forbidden`、`1.0 + 2.25 = 3.25x` 逻辑预算，以及 reservation/usage receipt；旧 ledger 的 `tmp/hfs` 只是逻辑拆分，不是两个物理盘。释放陈旧 reservation 后重试，不得绕过容量门禁。 |
| `translate_failed` / `translate_service_unavailable` | 用 `ssh-mini-mcap-status --summary` 和 task receipt 查 task-owned governed container；禁止检查/复用历史共享容器、裸跑 Docker/`mcap_service` 或在源码树构建。 |
| `alignment_failed`（含 pdcl_pyclip 字样）| 检查 `/home/mini/dnp_parking_dev_scripts/mcap_clean` 是否存在（已在 prepare 脚本里注入 sys.path 并显式报错）|
| `frame_alignment_mismatch`（不可重试）| 数据与 frame 对不上，**设计如此**：人工确认卡片 frame 是否填错或数据包不对应 |
| `report_build_failed` 且含"拒绝构建报告" | case_dir 解析异常（cases-root 防呆护栏触发），查 gate 的 case_dir 输出 |

**生产禁止手工跑管线。** 紧急分析必须从受控群 `@小助手 + 分析 + 唯一问题单` 发起并执行 `run_or_join`；普通用户不能要求新代。只有 operator allowlist 用户可显式 `rerun/debug`，且上一代执行和 required delivery 真正终态、默认 `3/600s` 限速通过后，才能建立下一代并重新经过 admission、容量 reservation、fixed CLI 和 canary/release evidence。公共 `vm_task_submit`、`ssh-mini-submit` 或直接运行 pipeline 都是阻断项。

**终态失败不能静默。** 当前候选会为 VM/解析/HTML `terminal_failed` 以及 VM 提交前 quarantine 原子创建问题单 failure effect，并为人工 source 创建原任务话题 failure effect；pre-submit quarantine 对外只发 `outbox_submission_quarantined`，内部错误不外泄，不生成虚假 HTML。required effect 未完成时不得 rerun/debug。5/15/60 分钟 outcome SLO 或一小时连续 3 次终态交付失败会背压新 outbox，但不能停止既有 delivery recovery。成功与失败都必须通过 marker 幂等、read-before/write/read-after 和真实飞书 canary 后才能生产放量；operator 监控、单测或用户主动查状态不能替代这一用户契约。

## 4.4 历史 Phase2 replay（非生产）

旧 Phase2 资料只用于理解历史 stage/checkpoint，不是当前生产恢复入口。任何 replay 都必须建立独立 task-id、落 `/mnt/tmp/<task_id>/`，并经当前 VM candidate 的 governed wrapper 和 release contract；不得直接使用 live repo 中的旧命令。

核心能力：
- 当前 runtime doctor 必须验证固定 `/usr/bin/python3`、hash-locked overlay、精确 dependency source/version/hash，并拒绝外部 PYTHONPATH/PYTHONHOME；旧 `rca_env.py` 结论不能替代该证明。
- `rca_checkpoint.py snapshot|restore` 可冻结/恢复 s3b/s5/s6 stage 产物；默认 fixtures 根：VM `/mnt/tmp/rca-fixtures`，用户可见 CIFS：`//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/rca-fixtures/`。
- Phase2 replay 只允许非生产、显式审批的隔离验证，并通过 `ssh-mini-mcap-run`/受治理 wrapper 进入；不得直接调用 pipeline。隔离 cases root 必须在 `/mnt/tmp/<task_id>/cases`，禁止触碰 production canonical cases root。
- `missing_prerequisite` / `stale_prerequisite` 是 Phase2 stage 契约 blocker：按消息补跑/恢复指定上游 stage，不要绕过。
- 生效语义按 fresh process identity 与生产 effect-chain 判定；不得仅凭“spawn-per-run”假设跳过 worker/service provenance。

历史 replay 命令已撤下，避免被误用于生产。需要 replay 时先建立独立 task-id，执行 `ssh-mini-resource --summary`，按 MCAP governed execution runbook 生成带 memory/CPU/PID/timeout 和 task-owned cleanup 的命令，并把 receipt 留在 `/mnt/tmp/<task_id>/`。

边界：Phase2 不解决 s5 `RoadTopoDebug`/protobuf schema 漂移类数据解码问题；这类问题登记到独立 s5 对齐健壮性 backlog，不混入 ops replay closure。

## 5. 审计位置（出问题先看这些）

| 内容 | 位置 |
|---|---|
| Kafka inbox/outbox/task/delivery effect | Host `~/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3`；运维只用 `mode=ro` + `query_only` 或受控 CLI，不手改 DB |
| 四个 resident health | 同目录 `health.json`、`outbox_dispatcher_health.json`、`delivery_collector_health.json`、`delivery_dispatcher_health.json` |
| Worker dispatch/终态 | VM `/home/mini/.hermes/worker-state/tasks/<task_id>/local-result.json` + task-bound dispatch/attestation receipt |
| VM service request/result | `/mnt/tmp/<submission_key>/rca_execution_request.json`、`rca_execution_attestation.json`、service result/provenance |
| 远程读取/转换清单 | `/mnt/tmp/<submission_key>/s2_remote_read/remote_read_receipt.json`、derived stream seal、后续 stage receipts |
| 容量证据 | storage admission v2 的单一 `/mnt/tmp` target + 同任务目录 derived reservation/lifecycle/meter receipts；三处 CIFS mount evidence 必须完全相等 |
| 最终报告与交付 | task-bound `index.html`、`report_data.json`、delivery manifest/contract/effect/read-after-write；归因保持 need_review |
| 发布 canary | Host release evidence 下 Kafka success、正式群 manual-origin success、manual terminal-failure 三组 receipt + sources；必须由 collector 只读生成并同时通过 gate |
| SQLite 上线证据 | 唯一 `RUN_ID` 目录中的 migration receipt、greenfield genesis/materialization receipt、prepared/installed/receipted journal、quarantine/restore receipt（如发生）、initial/completion writer-stop proof + TTL、maintenance/tombstone 状态 |
| Activation | `pnc_rca_activation.py status`、current epoch 两组 stage receipt/capsule/commit-marker binding、冻结 canary plan、exact slot/ledger、start/end fence 与 production confirmation 三件套；不得手改 DB |

## 6. 回归测试（改任何相关代码后必跑）

```bash
# Host：只在待发布的隔离 worktree 运行，不在 live runtime 上试改。
cd <host-candidate-worktree>
RCA_PLIST="$PWD/local.pnc.rca-kafka-consumer.candidate.plist"
RCA_PYTHON="$(plutil -extract ProgramArguments.0 raw -o - "$RCA_PLIST")"
test -x "$RCA_PYTHON"
"$RCA_PYTHON" -m pytest -q -o addopts='' \
  tests/gateway/test_pnc_rca_data_access.py \
  tests/gateway/test_pnc_rca_schema.py \
  tests/gateway/test_pnc_rca_control_store.py \
  tests/gateway/test_pnc_rca_delivery_store.py \
  tests/gateway/test_pnc_group_binding_gateway_dry_run.py \
  tests/gateway/test_pnc_global_issue_kafka_boundary.py \
  tests/tools/test_vm_task_tool.py \
  tests/tools/test_vm_task_service_admission.py \
  tests/scripts/test_pnc_rca_outbox_dispatcher.py \
  tests/scripts/test_pnc_rca_kafka_consumer.py \
  tests/scripts/test_pnc_rca_delivery_collector.py \
  tests/scripts/test_pnc_rca_delivery_dispatcher.py \
  tests/scripts/test_pnc_rca_activation.py \
  tests/scripts/test_pnc_rca_store_migration_drill.py \
  tests/scripts/test_pnc_rca_canary_collector.py \
  tests/scripts/test_pnc_rca_release_gate.py

# VM 测试可能超过 30 秒：在 clean candidate worktree 中经 detached submit 运行。
~/.local/bin/ssh-mini-resource --summary
~/.local/bin/ssh-mini-submit --task-id rca-candidate-regression --queue-if-blocked -- \
  bash -lc 'cd <vm-business-candidate-worktree> && python3 -m pytest -q api/g1q3_rca/tests'
```

不得用历史 passed 数量代替当前结果，也不得用 live repo、`ssh-mini-run` 长任务、手工 pipeline、真实下载或临时 Agent 会话做回归。发布 gate 还必须校验 Host/worker/VM 三个 clean commit、入口 hash、runtime overlay、容器 digest 和 canary source provenance。

## 7. 生产放量基线（2026-07-11 remote-read cutover）

1. 首次上线保持 submit/dispatch safe-off；先完成 broker metadata/T0、24h/200-case remote-reader soak、容量 horizon、clean BOM、受治理 canary。topic 必须由 broker metadata 确认精确大小写；当前 live `.env` 是 `feishu-project-workflow-event`，不得凭曾出现的 `feishu-project-workfLow-event` 交接字符串猜测。
2. 每单固定 `remote_read`、`allow_download=false`、`input_materialization=forbidden`；S2 只生成有 size/SHA/MCAP seal 的派生流。
3. Worker 必须 `direct_cli + agent_backend=none`，并产出绑定 commit、入口 hash、run-id/PID/dispatch receipt 的 attestation；任何 Agent/fallback 计数非零即阻断。
4. MCAP 转换必须按任务独立容器、显式 memory/CPU/PID/timeout、ownership cleanup；镜像必须 pin digest 后才能生产 promotion。
5. 任务产物固定 `/mnt/tmp/<submission_key>/`，对外 CIFS `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<submission_key>/`；禁止写 production canonical cases root。
6. 放量只按 `preauthorization -> safe_off -> preproduction -> preauthorized -> bounded -> production confirmation -> steady` 推进；正式人工 canary 只在 final chat subset、exact bounded slots 和同一 Gateway PID 上执行。回滚先关人工 intake 和 dispatch，再关 Kafka submit；不恢复绕过 durable outbox/control store 的旧群聊直提或 MDI 下载。
7. SQLite 按 live 事实二选一：existing v8/v5 必须有 BOM-pinned predecessor validator 与真实恢复；greenfield 必须有 genesis/materialization journal/receipt 和受控 quarantine/tombstone 回滚。control-plane materialization 不是问题输入物化，后者始终 `forbidden`。
8. 对 delivery dispatcher candidate plist 与安装后 plist 的精确 `ProgramArguments[0]` 解释器执行 `lark-oapi==1.5.3` 话题回复 API 探针；receipt 必须绑定 plist SHA、解释器 realpath 与探针结果，不能用开发 shell、其他 venv 或 lockfile 代替。
9. 若配置了 Feishu API poll，回滚窗口先冻结新的人工触发流量并保持 gateway 运行，等待 `~/.hermes/feishu_api_poll_state_v1.json` 中 `rollback_readiness.ready=true`、`rollback_readiness.pending_count=0`、`rollback_readiness.scan_continuation_count=0`，再关闭人工 intake 并停止 gateway；保存 sidecar 原件及 SHA-256 作为 evidence，旧 binary 可以忽略但不得删除。sidecar 缺失只有在配置和运行证据同时证明 API poll 从未启用时才允许。

受控群 `@小助手` 是与 Kafka 并列的正式人工入口，不是临时调试旁路，也不在回滚时永久下线。所谓“禁止旧群聊直提”仅指禁止绕过 durable outbox/control store 直接提交 VM、调用通用 Agent 或恢复 MDI 下载链路。

以下所有命令必须在同一个冻结 candidate root 下执行，并使用四个 candidate plist 共同绑定的解释器；candidate worktree 本身不要求携带 `.venv`。每次发布只允许一个全局唯一 `RUN_ID`，所有 evidence、capsule、receipt 和 machine source 都必须落在同一个 owner-only、no-clobber 子目录，禁止写到其父目录：

```bash
RCA_REPO_ROOT='<冻结且 clean 的 Host candidate 绝对路径>'
RCA_PLIST="$RCA_REPO_ROOT/local.pnc.rca-kafka-consumer.candidate.plist"
RCA_PYTHON="$(plutil -extract ProgramArguments.0 raw -o - "$RCA_PLIST")"
test -x "$RCA_PYTHON"
cd "$RCA_REPO_ROOT"

RUN_ID='<审批单号加 UTC 时间；全局唯一>'
EVIDENCE_ROOT="$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/release_evidence"
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
```

当前生产拓扑中 control schema v10 与 delivery schema v6 共用 `CONTROL_DB` 这一物理 SQLite 文件；四个 resident 的 `*_CONTROL_DB_PATH` 和 outbox 的 `DELIVERY_DB_PATH` 必须解析为同一绝对路径。这里的“同库”是受支持且被 gate 强制的原子事务边界，不是把问题输入下载或物化到本机。

### 7.1 SQLite bootstrap、迁移与回滚演练

只有 Store 物化、quarantine 或 restore 原子窗口需要同时停止 `ai.hermes.gateway` 与四个 RCA resident writer，并由发布编排生成 `pnc_rca_writer_stop_evidence_v1`。该 evidence 必须逐项记录五个 label 的 `pid_absent`、`health_state=stopped` 和观测时间；CLI 还会在演练前后独立执行 launchctl + psutil 进程探针，调用方 JSON 不能替代机器检查。物化完成且 maintenance marker 已由同一 journal 正常释放后，可启动 exact candidate Gateway；此后 preauthorization、preproduction、canary、production 到 steady 都不得重启或替换该 Gateway。四个 RCA resident 则保持停止到 epoch 已进入 `bounded_active`。

```bash
RUN_ID='<审批单号加 UTC 时间；全局唯一>'
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
MIGRATION_WORK_DIR="$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/migration_drill/$RUN_ID"
CONTROL_DB="$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
WRITER_STOP_EVIDENCE="$EVIDENCE_DIR/writer_stop_evidence.json"
CONFIG_SHA256='<release gate public config 的 exact SHA-256>'
QUARANTINE_DIR="$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/quarantine/$RUN_ID"

# 该命令只观察 launchd + psutil；不会 stop/kickstart/修改服务。
# 任一 writer 仍存在时返回 2，且不会生成新的停写证明。
"$RCA_PYTHON" scripts/pnc_rca_writer_stop_evidence.py \
  --evidence-dir "$EVIDENCE_DIR"

"$RCA_PYTHON" scripts/pnc_rca_store_migration_drill.py \
  --control-db "$CONTROL_DB" \
  --delivery-db "$CONTROL_DB" \
  --work-dir "$MIGRATION_WORK_DIR" \
  --evidence-dir "$EVIDENCE_DIR" \
  --writer-stop-evidence "$WRITER_STOP_EVIDENCE" \
  --receipt "$EVIDENCE_DIR/store_migration_receipt.json"

# Greenfield 只读 plan；不得创建 live DB。
"$RCA_PYTHON" scripts/pnc_rca_store_migration_drill.py \
  --control-db "$CONTROL_DB" --delivery-db "$CONTROL_DB" \
  --work-dir "$MIGRATION_WORK_DIR" --evidence-dir "$EVIDENCE_DIR" \
  --writer-stop-evidence "$WRITER_STOP_EVIDENCE" \
  --materialize-fresh-from-receipt "$EVIDENCE_DIR/store_migration_receipt.json" \
  --materialization-config-sha256 "$CONFIG_SHA256"

# 审批后以完全相同输入 apply；receipt 固定 no-clobber 写入 EVIDENCE_DIR。
"$RCA_PYTHON" scripts/pnc_rca_store_migration_drill.py \
  --control-db "$CONTROL_DB" --delivery-db "$CONTROL_DB" \
  --work-dir "$MIGRATION_WORK_DIR" --evidence-dir "$EVIDENCE_DIR" \
  --writer-stop-evidence "$WRITER_STOP_EVIDENCE" \
  --materialize-fresh-from-receipt "$EVIDENCE_DIR/store_migration_receipt.json" \
  --materialization-config-sha256 "$CONFIG_SHA256" \
  --release-id "$RUN_ID" --operator '<审批发布人>' --reason '<审批原因>' \
  --apply
```

迁移命令只读 configured source DB，`EVIDENCE_DIR`/`MIGRATION_WORK_DIR` 必须是本次唯一、owner-only、no-clobber 的 `RUN_ID` 目录，不得指向 live DB 目录或复用历史成功目录。existing v8/v5 路线必须显式提供 BOM-pinned predecessor validator，并由 migration v3 receipt 证明 backup、candidate、restore、旧 binary subprocess 与时长；任何 WAL/SHM/rollback-journal sidecar 都必须先由受控停写/checkpoint 流程消除，不能用 immutable read 忽略。greenfield 路线的 migration receipt `ok=true` 仍必须保留 `fresh_install_materialization_required` blocker；先运行只读 materializer plan，再以完全相同的 receipt/config/path、显式 release/operator/reason 执行 apply。崩溃后 maintenance marker 必须保留并由同一 journal/receipt 跨进程恢复，禁止删 marker 重来；稳定 live、quarantine 和 restore artifact 都必须是 owner-only、单链接文件。

每次 plan、apply 和崩溃恢复前都重新运行 writer-stop collector，并显式使用审批后的 `--max-writer-stop-age-seconds`（默认 900 秒）。CLI 在开始、持锁后、危险 unlink/install 前后和完成时重复机器进程探测；最终 quarantine/restore receipt 分别保存 `initial`、动作点与 `completion` 停写证明及 TTL。单次短调用可在 TTL 内复用同一 observation，跨进程或超时恢复必须刷新输入，禁止手改旧 proof。quarantine journal 固定为 `prepared -> linked -> unlinked -> tombstoned -> receipted`；restore journal 固定为 `prepared -> staged -> copied -> installed -> verified -> unfenced -> receipted`。JSON receipt/journal 和 stage-to-live hardlink 的中断态只允许按同 inode、owner、mode、link-count、内容及 DB genesis/inheritance 的唯一匹配恢复；歧义或额外链接保留现场并 fail closed。

Greenfield 回滚不是删除 DB。先重新停止五个 writer 并刷新 writer-stop evidence；quarantine/restore 都先 plan，审批后才在完全相同的 `RUN_ID`、config、receipt 和审计参数上加 `--apply`：

```bash
# Quarantine plan；apply 时追加 release/operator/reason 与 --apply。
"$RCA_PYTHON" scripts/pnc_rca_store_migration_drill.py \
  --control-db "$CONTROL_DB" --delivery-db "$CONTROL_DB" \
  --work-dir "$MIGRATION_WORK_DIR" --evidence-dir "$EVIDENCE_DIR" \
  --writer-stop-evidence "$WRITER_STOP_EVIDENCE" \
  --quarantine-fresh-from-materialization-receipt \
    "$EVIDENCE_DIR/fresh_install_materialization_receipt.json" \
  --fresh-rollback-quarantine-dir "$QUARANTINE_DIR" \
  --fresh-rollback-config-sha256 "$CONFIG_SHA256"

# Restore plan；只能消费上述 exact quarantine receipt。
"$RCA_PYTHON" scripts/pnc_rca_store_migration_drill.py \
  --control-db "$CONTROL_DB" --delivery-db "$CONTROL_DB" \
  --work-dir "$MIGRATION_WORK_DIR" --evidence-dir "$EVIDENCE_DIR" \
  --writer-stop-evidence "$WRITER_STOP_EVIDENCE" \
  --restore-fresh-from-quarantine-receipt \
    "$EVIDENCE_DIR/fresh_install_quarantine_receipt.json" \
  --fresh-rollback-quarantine-dir "$QUARANTINE_DIR" \
  --fresh-rollback-config-sha256 "$CONFIG_SHA256"
```

Quarantine 成功后 live DB 必须缺失、tombstone 必须保留、quarantine artifact 必须单链接；restore 成功后 live 与保留的 quarantine artifact 都必须各自单链接，tombstone/maintenance 只能由 receipted journal 精确释放。停写证据采集器本身绝不停止服务；实际 stop、物化、隔离、恢复均属于生产动作，只能在批准的单一发布窗口由唯一发布者执行。

### 7.2 Kafka metadata 只读证据

在启动 consumer、建立 consumer-group session 或决定 T0 之前，使用候选解释器采集精确 topic 的 broker metadata：

```bash
RUN_ID='<与 7.1 完全相同的发布 ID>'
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"

"$RCA_PYTHON" scripts/pnc_rca_kafka_preflight.py \
  --env-file "$HOME/.hermes/.env" \
  --output "$EVIDENCE_DIR/broker_metadata.json"
```

`--env-file` 是权威输入，不允许 ambient `HERMES_RCA_KAFKA_*` 静默覆盖；文件必须是 owner-only 的非 symlink UTF-8 常规文件，采集器以同一个只读 fd 完成 identity 检查和有界读取。evidence 只记录文件 identity 与去除密码后的配置哈希，不记录密码或密码哈希。owner 必须显式设置 `HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID` 和 `HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR`，不得依赖 bootstrap 地址暗示 cluster identity，也不得依赖代码猜测副本 SLO。采集器使用 pinned `kafka-python==3.0.7` 的单次精确 topic Metadata v12 响应同时读取 cluster identity、topic topology 和 topic authorized operations，不额外要求 cluster ACL；该请求强制 `allow_auto_topic_creation=false`、`include_topic_authorized_operations=true`，且 broker 返回的 cluster id 必须与权威 env 完全相等。随后仅对固定 Group `root_cause_analysis_agent` 执行一次 `DescribeGroups(include_authorized_operations=true)` 权限读取；client 可以先用无副作用的 `FindCoordinator` 定位该 group，但不得调用 OffsetFetch。只有 broker 返回的 cluster identity、精确 topic、从 0 连续的 partition、有效 leader、ISR 与 replicas 完全相等、零 offline replica、副本数达到显式策略，且 topic 与 group 都明确具备 `READ` + `DESCRIBE`、同时不具备 `WRITE` / `CREATE` / `DELETE` / `ALTER` / `ALTER_CONFIGS` / `CLUSTER_ACTION` / `IDEMPOTENT_WRITE` 等 mutation 权限时，才分别写 `topic_healthy=true`、`topic_authorized=true` 与 `group_authorized=true`。`broker_metadata.json` 是这两个资源的同源、同连接证据，不再依赖未实现的独立 `broker_acl_snapshot.json`。采集器不创建 consumer、不加入 consumer group，也不调用 subscribe、assign、poll、commit、OffsetFetch 或 ListOffsets；`DescribeGroups` 是精确 topic Metadata 之外唯一的数据/权限读取。每分区 T0 必须另由 owner 根据上线语义审定，写入 `HERMES_RCA_KAFKA_START_OFFSETS_JSON` 并形成独立 `t0_offsets.json`；不得从任何 metadata 或 group 结果自动推导 T0。

### 7.3 单事件 canary 证据采集

`canary_plan.json` 必须为 exact-shape `pnc_rca_canary_plan_v4`，固定 `admission_mode=direct_bounded`、`promotion_budget=0`、`slot_count=3`。槽位只能是 `kafka_success`、`manual_success`、`manual_terminal_failure`，每槽 `max_admissions=1`；入口分别为 `kafka_ingest/manual_admit/manual_admit`，结果分别为 `success/success/terminal_failed`。Kafka identity 只接受 exact event UID 并规范化绑定 topic/partition/offset；人工 identity 必须完整绑定 chat/requester/message/thread/canonical issue URL/`run_or_join`。三个 identity 与 submission 必须相互独立。plan raw SHA 在 preauthorization capsule 中冻结并由 preproduction 重验，之后 `authorize` 与 `transition-bounded` 都必须消费该 preproduction capsule。

旧 `scripts/g1q3_rca_e2e_smoke.py` 已退休，不得把它的合成结果当生产证据。正式 Kafka canary 必须在 `bounded_active` 下由已授权的 exact `kafka_success` identity 直接 admission，并原子消费该 slot；`safe_off/preauthorized` 阶段不落 raw、不提交 offset，也不得用 shadow promotion 替代。collector 本身不 admission、不消费 Kafka、不提交 VM 任务、不写飞书；旧 promotion 仅保留历史/隔离审计兼容。

先把 Chromium 生成的 smoke evidence 写到 collector 的固定 machine source 位置：

```bash
RUN_ID='<与 7.1 完全相同的发布 ID>'
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
RCA_ENV_FILE="$HOME/.hermes/.env"
SUBMISSION_KEY='<从已完成 control DB 记录确认的 exact submission key>'

"$RCA_PYTHON" scripts/pnc_rca_html_browser_smoke.py \
  --manifest '<本机只读 delivery_manifest.json>' \
  --contract '<本机只读 delivery_contract.json>' \
  --output "$EVIDENCE_DIR/machine_sources/$SUBMISSION_KEY/browser_smoke.json"
```

`RCA_ENV_FILE` 必须是当前用户持有、无软链/硬链且 group/other 无权限的 UTF-8 文件（通常为 `0600`）。collector 按 literal dotenv 读取，不展开 `${...}`；control DB 取 `HERMES_RCA_KAFKA_CONTROL_DB_PATH` / `HERMES_RCA_OUTBOX_CONTROL_DB_PATH` 且两者同时存在时必须一致，delivery DB 取 `HERMES_RCA_OUTBOX_DELIVERY_DB_PATH`，evidence/group receipt 分别取 `HERMES_RCA_CANARY_EVIDENCE_DIR` / `HERMES_RCA_CANARY_GROUP_BINDING_RECEIPT_DIR`，正式群集合取 `HERMES_RCA_MANUAL_CHAT_IDS`。命令行显式参数优先于 env-file。

然后从 control DB 只读确认 exact event 已绑定 current epoch 的 `kafka_success` consumed ledger 与 immutable source ID，对这个 source 先 dry-run，再显式写本机 release evidence：

```bash
SOURCE_ID='<control DB 中该 event 对应的 g1q3-rca-source-v1-...>'

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$SOURCE_ID" \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --dry-run

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$SOURCE_ID" \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --write
```

`--write` 先以 `0600`、`O_EXCL` 发布 `canary_receipt.<commit_id>.json` 与 `canary_receipt_sources.<commit_id>.json` 两个不可变 generation，fsync 后才原子切换固定 `canary_receipt_commit.json` manifest；CLI 会返回 `evidence_commit_id` 和三个文件名。并发不同 commit 直接 fail closed，同一 commit 幂等复用；旧的 `canary_receipt.json` / `canary_receipt_sources.json` 不再是 production gate 输入。所有执行事实仍为只读采集。collector 不提供手工 receipt、payload 或 source-pack 入参；任一 control/delivery 重复记录、direct bounded epoch/ledger/slot/source identity 绑定不一致、VM 路径逃逸/软链、receipt hash、task/run/submission/artifact-set 绑定不一致都 fail closed。

collector 对远端 JSON/产物哈希采用单次 `ssh-mini-agent run_py_json`，请求总上限固定 1.6GB；产物使用 sealed inventory 的精确 size，按 16MiB/s 推导远端 deadline（最大 110s），wrapper/Host 各只增加 5s grace，整体不超过生产 120s 边界。预算超限必须在启动远端进程前拒绝；远端 deadline、wrapper timeout 或 Host timeout 统一归类为 `remote_source_reader_timeout`，不得误报为依赖不可用或回退 MDI。

正式 production 还必须选取一条由 G1Q3 RCA 正式群定向 `@小助手` 独立创建、成功完成分析并回到原任务话题的 generation-1 manual source。Kafka-origin + manual join、测试群 source、rerun/debug、失败或隔离样本都不能替代：

```bash
MANUAL_SUCCESS_SOURCE_ID='<正式群 run_or_join created source 的 g1q3-rca-source-v1-...>'

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$MANUAL_SUCCESS_SOURCE_ID" \
  --manual-success \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --dry-run

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$MANUAL_SUCCESS_SOURCE_ID" \
  --manual-success \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --write
```

这一步独立发布 `manual_success_canary.<commit_id>.json`、`manual_success_canary_sources.<commit_id>.json`，最后切换 `manual_success_canary_commit.json`，不会覆盖 Kafka success receipt。gate 要求 manual 与 Kafka 的 source、business、submission 和 artifact set 相互独立，并要求 manual success 同时完成 `feishu_issue_comment` 与绑定原 topic 的 `feishu_thread_reply`。

每条 group-binding decision 只允许写一个 `{UTC-date}-{source-identity-sha256}.jsonl` 单事件不可变原件：owner-only 根目录精确 `0700`，文件精确 `0600`，`O_EXCL` 创建、单行 JSONL、file+directory fsync，重复 message/source event 必须在 admission 前 fail closed。旧的按日追加 JSONL、追加后的文件、软链/硬链、非当前 UID、非 canonical root 或多个候选都不能作为新 canary 原件。collector 只按 source identity 计算当前/前一 UTC 日两个精确文件名，不扫描目录。

accepted manual 原件必须携带 Gateway 启动时冻结的 `ai.hermes.gateway` runtime identity；该 identity 的 public config 同时绑定完整 workflow policy、policy SHA、creation rule version、Kafka outbox high-watermark、群/operator allowlist 与限速，并与实际 manual admission 共用同一份启动快照。production gate 会把 identity 与候选 build manifest、same-fd Gateway 文件快照、已安装 plist、launchd **已加载**的 program/arguments/working-directory/environment、批准的 venv 解释器及 `gateway/psutil/dotenv` import origins、psutil PID/create-time/exe/cwd/cmdline 逐项对齐。manual success 与 terminal-failure 两条 canary 必须来自同一个仍在运行的 Gateway identity；Gateway 一旦重启，两条都要在新 PID 上重新执行和采集。

人工入口还必须选取一条在原任务话题真实完成失败投递的 manual source，采集专用终态 canary：

```bash
MANUAL_FAILURE_SOURCE_ID='<g1q3-rca-source-v1-...>'

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$MANUAL_FAILURE_SOURCE_ID" \
  --terminal-failure \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --dry-run

"$RCA_PYTHON" scripts/pnc_rca_canary_collector.py \
  --source-id "$MANUAL_FAILURE_SOURCE_ID" \
  --terminal-failure \
  --env-file "$RCA_ENV_FILE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --write
```

这一步发布 v2 `manual_terminal_failure_canary.<commit_id>.json`、`manual_terminal_failure_canary_sources.<commit_id>.json`，最后切换 `manual_terminal_failure_canary_commit.json`。terminal receipt 必须携带 control store 中的完整 admission；gate 会把 rule/project/simple-name/type、manual-origin 或 observer transport 语义绑定到当前 active workflow policy，并重新回读不可变授权原件。来源同时绑定当前配置的 control/delivery SQLite 绝对路径与实时 device/inode，并要求 `Gateway process create <= authorization decision <= source created/bound <= terminal <= materialized <= completed <= collected`、整链位于 freshness window；仅伪造一个新 `collected_at` 不能通过。

### 7.4 执行 release gate

topic 只能取自已验证的 `broker_metadata.json`，不能在命令中凭记忆填写。每个阶段单独运行；只有前一阶段 receipt `ok=true` 且 blockers 为空，才准备下一阶段所需 live evidence。

```bash
RUN_ID='<与 7.1 完全相同的发布 ID>'
EVIDENCE_DIR="$EVIDENCE_ROOT/$RUN_ID"
EXPECTED_TOPIC='<broker_metadata.json 中经 owner 确认的精确 topic>'
EXPECTED_RULE_VERSION='issue-created-v1'

# shadow 仅供隔离/legacy 审计，不创建 production epoch。
# 正式顺序固定为 preauthorization -> preproduction -> canary -> production。
MODE='preauthorization'
PREAUTH_RECEIPT="$EVIDENCE_DIR/release_gate_${MODE}_${RUN_ID}.json"
"$RCA_PYTHON" scripts/pnc_rca_release_gate.py \
  --mode "$MODE" \
  --evidence-dir "$EVIDENCE_DIR" \
  --env-file "$HOME/.hermes/.env" \
  --expected-topic "$EXPECTED_TOPIC" \
  --expected-rule-version "$EXPECTED_RULE_VERSION" \
  --receipt "$PREAUTH_RECEIPT"

PREAUTH_CAPSULE="${PREAUTH_RECEIPT%.json}.activation-preauthorization.json"
CONTROL_DB='<当前 production control.sqlite3 绝对路径>'

# 两次命令输入完全相同；第一条 plan，第二条才 apply create safe_off。
"$RCA_PYTHON" scripts/pnc_rca_activation.py --control-db "$CONTROL_DB" create \
  --preauthorization-capsule "$PREAUTH_CAPSULE" \
  --operator '<审批发布人>' --reason '<审批单号和创建原因>'
"$RCA_PYTHON" scripts/pnc_rca_activation.py --control-db "$CONTROL_DB" create \
  --preauthorization-capsule "$PREAUTH_CAPSULE" \
  --operator '<审批发布人>' --reason '<审批单号和创建原因>' --apply

PREPROD_RECEIPT="$EVIDENCE_DIR/release_gate_preproduction_${RUN_ID}.json"
"$RCA_PYTHON" scripts/pnc_rca_release_gate.py \
  --mode preproduction --evidence-dir "$EVIDENCE_DIR" \
  --env-file "$HOME/.hermes/.env" --expected-topic "$EXPECTED_TOPIC" \
  --expected-rule-version "$EXPECTED_RULE_VERSION" \
  --preauthorization-capsule "$PREAUTH_CAPSULE" \
  --receipt "$PREPROD_RECEIPT"

PREPROD_CAPSULE="${PREPROD_RECEIPT%.json}.activation-preproduction.json"
"$RCA_PYTHON" scripts/pnc_rca_activation.py \
  --control-db "$CONTROL_DB" transition-preauthorized \
  --preproduction-capsule "$PREPROD_CAPSULE" \
  --operator '<审批发布人>' --reason '<审批单号和预生产确认>'
"$RCA_PYTHON" scripts/pnc_rca_activation.py \
  --control-db "$CONTROL_DB" transition-preauthorized \
  --preproduction-capsule "$PREPROD_CAPSULE" \
  --operator '<审批发布人>' --reason '<审批单号和预生产确认>' --apply
```

随后用 `pnc_rca_activation.py authorize` 逐条绑定三种 exact identity，并以 `transition-bounded` 的 plan/apply 进入 bounded。Kafka identity 使用 `--event-uid`，两个人工 identity 使用 owner-only JSON 文件和 `--manual-identity-json`。三个 authorize 命令和 transition-bounded 的 plan/apply 都必须同时携带 exact `--epoch-id`、`--preproduction-capsule "$PREPROD_CAPSULE"`、operator、reason；CLI 会先按 capsule 比对 identity，再在 bounded 转换前逐槽回读授权 hash。不要用 shadow promotion 代替 direct bounded canary。

首次 bootstrap 在 bounded 后还不能启动 resident。必须先对下列命令执行同参数 plan/apply；CLI 在 plan 与全局排他锁内都校验 deadline 至少还剩 7 天 6 小时，再发布 create-once producer receipt，并验证空 ledger 的 runtime 为 `BOOTSTRAP_PRODUCTION` ready。崩溃后重跑按 receipt 的历史 `activated_at` 校验并只接受原字节，不能因重跑时间变晚误换 receipt；禁止删除、覆盖或在 `transition-steady` 时补建：

```bash
ACTIVE_BINDING="$(dirname "$CONTROL_DB")/active-release-binding.json"
LIVE_ENV="$HOME/.hermes/.env"
RELEASE_ID='<candidate HERMES_RCA_PROD_RELEASE_ID>'
BOOTSTRAP_EPOCH_ID='<candidate HERMES_RCA_PROD_BOOTSTRAP_EPOCH_ID>'

for APPLY in '' '--apply'; do
  "$RCA_PYTHON" scripts/pnc_rca_activation.py --control-db "$CONTROL_DB" \
    prepare-bootstrap-production \
    --epoch-id '<current activation epoch id>' \
    --preproduction-capsule "$PREPROD_CAPSULE" \
    --active-release-binding "$ACTIVE_BINDING" --live-env "$LIVE_ENV" \
    --release-id "$RELEASE_ID" --bootstrap-epoch-id "$BOOTSTRAP_EPOCH_ID" \
    --operator '<审批发布人>' --reason '<审批单号和 bootstrap producer 原因>' \
    $APPLY
done
```

只有 apply 返回 `producer_receipt_present=true`、producer receipt SHA/fingerprint 非空且随后 Outbox health 的动态 capacity state 为 `BOOTSTRAP_PRODUCTION` 或 `STEADY_READY`，才一次性启动四个 RCA resident。

Canary gate 必须消费同一个 preproduction capsule。首次 bootstrap 使用 `canary_bootstrap` / `production_bootstrap`；只有容量已完成 generation 2 ratchet 的后续 release 才使用 `canary` / `production`。三条真实 canary 完成且 health 显示 `state=activation_frozen` 后，production 先只读采集 canonical candidate，再运行完整 gate。普通 production 模式强制要求 `--receipt`；唯一例外是只读 `--collect-activation-production-candidate`。缺少 receipt 直接返回 2，红灯 gate 不创建 receipt、capsule 或 commit marker：

```bash
CANARY_RECEIPT="$EVIDENCE_DIR/release_gate_canary_bootstrap_${RUN_ID}.json"
"$RCA_PYTHON" scripts/pnc_rca_release_gate.py \
  --mode canary_bootstrap --evidence-dir "$EVIDENCE_DIR" \
  --env-file "$HOME/.hermes/.env" --expected-topic "$EXPECTED_TOPIC" \
  --expected-rule-version "$EXPECTED_RULE_VERSION" \
  --preproduction-capsule "$PREPROD_CAPSULE" --receipt "$CANARY_RECEIPT"

"$RCA_PYTHON" scripts/pnc_rca_release_gate.py \
  --mode production_bootstrap --evidence-dir "$EVIDENCE_DIR" \
  --env-file "$HOME/.hermes/.env" --expected-topic "$EXPECTED_TOPIC" \
  --expected-rule-version "$EXPECTED_RULE_VERSION" \
  --preproduction-capsule "$PREPROD_CAPSULE" \
  --collect-activation-production-candidate \
    "$EVIDENCE_DIR/activation_production_candidate.json"

PRODUCTION_RECEIPT="$EVIDENCE_DIR/release_gate_production_bootstrap_${RUN_ID}.json"
"$RCA_PYTHON" scripts/pnc_rca_release_gate.py \
  --mode production_bootstrap --evidence-dir "$EVIDENCE_DIR" \
  --env-file "$HOME/.hermes/.env" --expected-topic "$EXPECTED_TOPIC" \
  --expected-rule-version "$EXPECTED_RULE_VERSION" \
  --preproduction-capsule "$PREPROD_CAPSULE" --receipt "$PRODUCTION_RECEIPT"

CAPSULE="${PRODUCTION_RECEIPT%.json}.activation-confirmation.json"
"$RCA_PYTHON" scripts/pnc_rca_activation.py \
  --control-db "$CONTROL_DB" confirm \
  --operator '<审批发布人>' \
  --reason '<审批单号和本次确认原因>' \
  --confirmation-capsule "$CAPSULE"
"$RCA_PYTHON" scripts/pnc_rca_activation.py \
  --control-db "$CONTROL_DB" confirm \
  --operator '<审批发布人>' \
  --reason '<审批单号和本次确认原因>' \
  --confirmation-capsule "$CAPSULE" \
  --apply
```

preauthorization、preproduction 与 production 成功都采用三件套逻辑原子发布：receipt、stage/confirmation capsule、sibling `*.commit.json`。只有 marker `publication_complete=true` 且精确绑定 receipt/capsule 的绝对路径、长度、raw SHA、report fingerprint、transition SHA 与 pair SHA，capsule 才可消费；receipt/capsule 单独存在均不算提交。相同发布尝试必须复用原 receipt 路径并按原字节恢复缺失成员；receipt 缺失而 capsule/marker 已存在、任一字节冲突或篡改均 fail closed。不同发布尝试使用新 `RUN_ID`。三件套均为 no-clobber，不得覆盖或删除旧成功证据。

gate 返回 `0` 才表示该阶段通过；返回 `2` 或 receipt `ok=false` 都是硬阻断，不得通过改 receipt、跳检查或直接开 submit/dispatch 绕过。canary/production 还要求 `build_manifest.json`、`cutover_plan.json`、`store_migration_receipt.json`、remote-reader health/soak、capacity；production 另外要求 Kafka success、正式群 manual-origin success 与人工终态失败三组 canary。production gate 的 runtime final barrier 在所有 receipt/source/build 检查之后执行四个完整交错轮次，每轮重验 Gateway、四个 resident 与 host clean commit/完整 critical-file map；最终收口顺序固定为 source evidence → resident → Gateway → host。四轮 barrier 属于 gate；随后 `confirm` 在 `bounded_active` 上另执行两轮 runtime continuity recheck，顺序为 Gateway+4 residents -> Kafka freeze/freshness -> Gateway+4 residents。Gateway PID/create-time/loaded runtime/plist/launchctl 与 resident PID/create-time/boot/exe/cwd/cmdline/env/loaded-runtime/plist 任一变化都拒绝确认。中途 deploy、restart、plist/env/解释器或证据 generation 变化都必须红灯。缺任一证据都不能晋级。`confirmed` 期间逐条 reconcile 合法冻结区间内的 bound shadow；全局 shadow 清零后，再用下列 `transition-steady` 的同参数 plan/apply 完成业务 steady，不能把 `confirm` 当成最终放量。该命令只重验已经发布的 producer receipt，缺 receipt 时 fail closed，不会晚建：

```bash
for APPLY in '' '--apply'; do
  "$RCA_PYTHON" scripts/pnc_rca_activation.py --control-db "$CONTROL_DB" \
    transition-steady --epoch-id '<current activation epoch id>' \
    --active-release-binding "$ACTIVE_BINDING" --live-env "$LIVE_ENV" \
    --release-id "$RELEASE_ID" --bootstrap-epoch-id "$BOOTSTRAP_EPOCH_ID" \
    --operator '<审批发布人>' --reason '<审批单号和业务 steady 原因>' \
    $APPLY
done
```

### 7.5 容量 generation 2 ratchet 与崩溃恢复

业务 `steady_active` 后继续按 bootstrap 限额收集样本。只有 ledger 至少 20 条、跨度至少 7 天、最大 gap/最新样本/签名/Host-VM-terminal-delivery binding 全部通过时，owner 才签发一小时内有效的 exact steady authorization。普通转换先 plan 再 apply：

```bash
CAPACITY_AUTH='<owner-only signed steady authorization 绝对路径>'
BUSINESS_ACTIVATION_EPOCH_ID="$(
  "$RCA_PYTHON" scripts/pnc_rca_activation.py \
    --control-db "$CONTROL_DB" status | \
  "$RCA_PYTHON" -c 'import json,sys; e=json.load(sys.stdin)["result"]["activation"]["current_epoch"]; assert isinstance(e,dict) and e.get("state")=="steady_active"; print(e["epoch_id"])'
)"
"$RCA_PYTHON" scripts/pnc_rca_capacity_transition_executor.py \
  --control-db "$CONTROL_DB" status
"$RCA_PYTHON" scripts/pnc_rca_capacity_transition_executor.py \
  --control-db "$CONTROL_DB" transition-steady \
  --authorization "$CAPACITY_AUTH" \
  --business-activation-epoch-id "$BUSINESS_ACTIVATION_EPOCH_ID" \
  --operator '<容量审批人>' --reason '<容量审批单号和 generation 2 原因>'
"$RCA_PYTHON" scripts/pnc_rca_capacity_transition_executor.py \
  --control-db "$CONTROL_DB" transition-steady \
  --authorization "$CAPACITY_AUTH" \
  --business-activation-epoch-id "$BUSINESS_ACTIVATION_EPOCH_ID" \
  --operator '<容量审批人>' --reason '<容量审批单号和 generation 2 原因>' \
  --apply
```

一旦 `steady-intent.json` 已 create-once，后续崩溃恢复只能执行 `recover --apply`；恢复逻辑从 intent 读取冻结的 authorization/operator/reason，禁止 ambient 替换或自签。intent/auth/receipt/marker/bundle 任一冲突、缺失顺序非法或 DB CAS 漂移都保持 fail closed。恢复完成后同时要求 DB `generation=2/state=STEADY_ACTIVE`、五件证据哈希闭合、Outbox health 动态投影为 steady；不修改业务 Activation，不改已冻结 env，也不要求 resident 重启。

```bash
"$RCA_PYTHON" scripts/pnc_rca_capacity_transition_executor.py \
  --control-db "$CONTROL_DB" recover
"$RCA_PYTHON" scripts/pnc_rca_capacity_transition_executor.py \
  --control-db "$CONTROL_DB" recover --apply
```

### 7.6 离线控制面压力诊断

`pnc_rca_offline_pressure_harness.py` 只在显式临时 SQLite 上验证双入口合成 arrival、claim/retry/lease recovery、背压恢复、delivery uncertain reconcile、circuit 和本地 fake exact-once。它不访问 Kafka、飞书、VM、URL、LLM/HTML 或 launchd，不能替代 24h remote-reader soak、真实 canary 或 production gate。

```bash
PRESSURE_DIR="$(mktemp -d /tmp/pnc-rca-pressure.XXXXXX)"
"$RCA_PYTHON" scripts/pnc_rca_offline_pressure_harness.py run \
  --profile burst-1000 \
  --control-path "$PRESSURE_DIR/control.sqlite3" \
  --output-path "$PRESSURE_DIR/report.json"
shasum -a 256 "$PRESSURE_DIR/report.json"
```

2026-07-13 主线程独立候选结果：1000/1000 case、1200/1200 effect，7.917 case/s、126.307 秒排空，SQLite busy/locked=0、duplicate remote effect writes=0，报告 SHA256 `a5a9d31172fa9f01ae5b5f90dab0eef9178ec4fff2356e387f26ab23f7b11797`。这只是本机候选控制面观测，不是 live 性能结论。

### 7.7 Remote-reader 24h soak

VM 统一凭据入口是 `/home/mini/.hermes/service.env`（owner `mini`、`0600`）；兼容路径 `/home/mini/.openclaw/service.env` 必须与它保持同 device/inode。RCA fixed child 只允许传递 `PDCL_BASE_URL`、`STS_UID`、`STS_SECRET_KEY`、`PDCL_CACHE_MOUNT_POINT` 四键，并在 admission/Popen 前按存在性 fail closed；health 只报告 required/present/missing key name，禁止输出值。实装 `pdcl_dss==0.1.44` 读取 `PDCL_BASE_URL`，旧 `PDCL_URL`/`PDCL_HFS_SERVER` 不能替代 endpoint，也不得作为 ready 证明。worker 每次 child spawn 重新读取统一 env；部署新的 worker binary 仍按生产 effect chain 重启一次，后续 env 更新对新 child 生效。

正式 workload 必须由 PDCL/data owner 只读导出 `pnc_rca_remote_reader_soak_manifest_v1`：至少 200 个唯一 work item，ACC/LCC/AEB_FCW/DNP 各至少 50，clip/event 均存在且 `clip -> RemoteClipReader`、`event -> RemoteEventReader`。原字段只以 SHA-256 绑定；禁止把 MDI 命令、本地/NAS/MCAP path、下载 URL 或凭据写入 manifest。`source.artifact_sha256` 只是 producer locator，不是 manifest 自校验 hash；production 仍要求 data-owner provenance/receipt。禁止用 raw/path 猜测或复用 archive-review candidate 凑数。

正式 producer 是 `scripts/rca_issue_workload_export.py`。census 阶段只允许官方 `meegle` 读取 `work_item_id`、`field_e776bb`、`field_93aa63`，按稳定排序完整分页，并只持久化分类聚合、拒绝原因、查询/session 哈希和 remote-reference 计数；不得持久化原始 issue payload、PDCL 字段、description、attachment 或 session ID。manifest 阶段还必须满足：producer module 已提交且 clean、完整 source count 在所有页不漂移、domain mapping 与 live taxonomy SHA-256 完全一致、mapping 和独立 approval receipt 均为 canonical JSON/owner-only `0600`，且 approval receipt 逐字节 SHA 与 `mapping_rules_sha256` 双重绑定。四份 producer 合同见 `docs/pnc/schemas/rca_issue_workload_export_census_v1.schema.json`、`docs/pnc/schemas/rca_issue_domain_mapping_v1.schema.json`、`docs/pnc/schemas/rca_issue_domain_mapping_approval_v1.schema.json` 和 `docs/pnc/schemas/rca_issue_workload_export_receipt_v1.schema.json`；producer 与这四份 schema 均必须进入 release BOM。

2026-07-14 12:38 +08:00 的 authenticated live census（G1Q3 `t03o4q` / `issue`）完整稳定读取 5,206/5,206 条非空 PDCL 记录，得到 1,239 个可解析 work item、1,141 个唯一 remote reference（81 clip、1,158 event candidates）；这已替代旧的“无可用 workload source”结论。它仍不是生产 manifest：live 64 个 `field_e776bb` leaf 中不存在名为 `DNP` 的 option，且 VM 当前只识别显式 `DNP`，所以任何 HNOA/TSI/TSR/HMI/TJA 等组合都必须由 PDCL/data owner 在 taxonomy-bound mapping receipt 中明确批准，不能按父类或样本数推断。

真实任务必须通过 long-task wrapper，work/output 仍落 `/mnt/tmp/<task_id>/`。唯一例外是 exact Host `remote_reader_health.json`：`/mnt/tmp` 为 CIFS、mode 会被合成为 `0755`，无法满足 collector 的 owner-only 检查；它必须落 VM 本地 `0700` 私有目录并保持 regular、same-euid、`0600`、no symlink，再以绝对路径传入。collector 会以 `O_NOFOLLOW` 单次快照逐字节绑定它、dependency doctor、candidate commit/tree、vendored schema SHA `1979a850be44ca190f7d468b2d2e3f1cc939fe755eb770ab9679403701c415fa`，任一不一致在开始前 fail closed：

```bash
SOAK_TASK_ID='rca-remote-reader-soak-<审批号>'
VM_CANDIDATE_ROOT='/mnt/tmp/rca-remote-read-soak-20260713/worktree'
SOAK_ROOT="/mnt/tmp/$SOAK_TASK_ID"
SOAK_MANIFEST="$SOAK_ROOT/input/workload-manifest.json"
SOAK_PRIVATE_ROOT="/home/mini/.hermes/shared-state/rca-soak-private/$SOAK_TASK_ID"
SOAK_HEALTH="$SOAK_PRIVATE_ROOT/remote_reader_health.json"
SOAK_OUTPUT="$SOAK_ROOT/output"

~/.local/bin/ssh-mini-submit --task-id "$SOAK_TASK_ID" -- \
  /usr/bin/python3 \
  "$VM_CANDIDATE_ROOT/api/g1q3_rca/scripts/run_remote_reader_soak.py" \
  --manifest "$SOAK_MANIFEST" \
  --remote-reader-health-evidence "$SOAK_HEALTH" \
  --output-dir "$SOAK_OUTPUT"
```

对外证据路径必须同时记录 `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`。成功 artifact 必须是 v4、24 个小时 bucket 每桶至少 4 单、四域/两 reader 配额满足、并发 4 累计至少 900 秒且最长连续至少 300 秒、所有 case receipt 完整、stream cache 最终保留 0 字节。preflight/execution failure 只能保留 failure artifact，不能生成或手工补 `remote_reader_soak.json`。

Host formal gate 必须同时消费六份证据，缺一即硬阻断：VM 生成的 `remote_reader_soak.json`，以及 producer/data owner 的五份**原始 canonical 输入字节** `remote_reader_soak_manifest.json`、`remote_reader_workload_census.json`、`remote_reader_domain_mapping.json`、`remote_reader_domain_mapping_approval.json`、`remote_reader_workload_export_receipt.json`。复制过程不得 parse/re-serialize；Host 目标必须全部是 owner-only `0600`、regular、same-euid、single-link、no symlink，父 evidence 目录不得 group/world-writable。gate 会把 producer commit/module SHA 与已验证 Host build/BOM 交叉核验，并逐字节验证 census、taxonomy、mapping、独立审批、manifest 和 export receipt 的双向哈希关系。发布 receipt 同时记录两个 manifest hash：

- raw-file SHA-256：覆盖 canonical JSON 后的唯一末尾 LF，用于 Host evidence file identity；
- canonical manifest SHA-256：覆盖不含末尾 LF 的 canonical JSON bytes，必须等于 `remote_reader_soak.json.workload_manifest.sha256`。

Host gate 会逐 case 重算 manifest record SHA、soak record SHA 和 domain-separated Merkle root，并发布 `manifest_case_binding_sha256 = H(manifest_sha256, case_merkle_root, manifest source)`；这不能替代 data-owner provenance，只用于阻止 manifest/source 与 case Merkle 被拆开替换。

### 7.8 v0.18.2 RCA 移植与验证边界

RCA 封板候选必须作为冻结 live 基线 `711a3e301710fef456627d5026039030ad096bf7` 到封板 `8ca23307c3a66bf64878f35b532a871f13a86928` 的完整业务 delta 移植到 v0.18.2 `a20fefaff110239bdb336607ba94c522f2b4b6c9`；不能只 cherry-pick 最后一个 commit。该 delta 为 62 个 commit、139 个路径。Feishu 在 18.2 已从 `gateway/platforms/feishu.py` 迁到 `plugins/platforms/feishu/adapter.py`，必须先把旧路径映射到插件路径再用明确 base 做 rename-aware `ort` merge，否则普通 three-way 会漏掉 durable inbox/API poll sidecar。

移植结果必须同时保留两组契约：

- v0.18.2：插件打包、SDK executor、record-only outbound、startup lookback、cooperative cancellation、legacy direct-fetch 兼容。
- RCA：两阶段 durable inbox、持久 API cursor/pending/hole、ID 优先且不可用 name 降级的机器人 mention 身份、fixed-group command-directed、canonical issue link、有限 Lark timeout，以及统一 Kafka/manual admission。

旧 `pnc_g1q3_governance_rca.py` 下载协调器永久退役；兼容入口只能返回 `g1q3_rca_legacy_download_coordinator_retired`，不得由 env 重新打开。Host 本地报告只有在内容、gate 和可验证的 recipient pickup surface 同时成立时才能进入 `done`。测试必须使用临时 `HERMES_HOME`，并通过 `HERMES_OUTBOUND_CENSUS_ROOT` 指向 SHA-256 已验证的 v0.18.2 outbound census；禁止把测试 sidecar 写入 live home。

本节只定义 **RCA 业务 delta 在 v0.18.2 上的生产就绪移植**。v0.14.6 全量本机能力向 v0.18.2 的继承、覆盖率和通用功能回归属于独立任务，不能据此扩大 RCA release BOM，也不能把另一任务的通过结论替代 RCA gate。

## 8. 当前 NO-GO 与长期专项

- 2026-07-14 00:51+08 以 live `/Users/songying/.hermes/.env` 运行只读 Kafka preflight，在连接 broker 前因缺 `HERMES_RCA_KAFKA_EXPECTED_CLUSTER_ID` fail-closed；键名盘点还缺 `HERMES_RCA_KAFKA_MIN_REPLICATION_FACTOR`。因此 2026-07-13 的 TCP/SASL 观察只保留为历史证据；当前仍未知 cluster ID、精确 topic casing/topology、topic/group ACL、真实 creation fixture 和每分区 T0，属于上线 P0 NO-GO。不得猜密码、listener、topic 大小写或 offset。
- 2026-07-14 00:51+08 只读 VM 探针确认统一 `/home/mini/.hermes/service.env` 为 `mini:0600` regular file，兼容路径与其同 device/inode，但 `PDCL_BASE_URL`、`STS_UID`、`STS_SECRET_KEY`、`PDCL_CACHE_MOUNT_POINT` 四键全部缺失。candidate remote-reader entrypoint、adapter、锁定 wheel/manifest 均存在；仍需四键和 data-owner 合法 200-case manifest 后才能启动 v4 24h soak。
- `/mnt/tmp` 派生产物容量/保留周期 horizon 必须覆盖目标吞吐并有自动 reservation release；历史容量估算不能替代 fresh proof。
- 2026-07-14 00:52+08 删除批次已回收 `223287304192` bytes，VM root 可用约 `528.93 GiB`，物理容量已超过 `rca_prod` 400 GiB 与 bootstrap 464 GiB 阈值；旧 333.4 GiB 容量卡点已 superseded。当前 `rca_prod` 仍因 capacity-model receipt 缺失而拒绝，bootstrap 仍因 owner authorization 缺失而拒绝；不得用 bypass、改资源类或复用 legacy MCAP 容器绕过。
- VM 固定 runtime overlay、所有依赖 source/hash、MCAP image digest 和 task-owned isolation 必须进入 release BOM；user-site 依赖只可作为逐文件指纹锁定的受控过渡。
- HTML 终端改造必须与 delivery manifest、递归 CSS/JS 依赖闭包、HTTP read-after-write 和 Chromium smoke 一起验收。
- Meegle 短期登录态仍是可用性风险；MCP auto-degrade 必须保留脱敏来源证据，长期应换服务级凭据/稳定 read API。
- 首次上线前必须完成一个 bounded exact `kafka_success` direct admission 的真实 governed canary，并由 collector 生成 receipt + source provenance；preauthorized shadow/promotion 或合成 smoke 不可替代。
- 人工入口必须先在测试群完成真实定向 @ 的 `run_or_join`、Kafka/manual 双入口竞态和原话题交付，再在正式群同一个当前 Gateway PID 上分别完成独立 manual-origin success 与真实失败终态 canary；source receipt 必须绑定 live SQLite path/device/inode、Gateway build/process identity 和单调新鲜时间线。未完成前保持 `HERMES_RCA_MANUAL_INTAKE_ENABLED=false`。
- live admission HMAC、release/epoch authority、active binding 与最终 owner/cutover approval 尚未物化；当前五个入口/派发 activation 开关保持 safe-off，候选测试通过不等于可启动 resident。
- control v8 → v10 / delivery v5 → v6 迁移备份恢复 receipt、五个 writer-stop 机器证明、四个 resident health 精确 schema、5/15/60 分钟 delivery-outcome SLO 与连续失败背压尚未形成 live 证据前，不得放量。
- 初始放量保持单事件/低并发，按 error rate、backlog、remote latency、VM pressure、容量和 delivery circuit 阶梯推进；任一硬阈值触发即关闭 dispatch/submit。

## 9. 接力 agent 快速上手顺序

1. 读 `/Users/songying/.hermes/BRIDGE.md`（来源分类纪律）
2. 读本文第 7 节 + 设计文档“当前有效设计”；历史附录不作为进度真相
3. 在隔离 candidate worktree 跑第 2 节健康检查 + 第 6 节回归测试，确认基线
4. 看最近 receipt（第 5 节）了解最近发生了什么
5. 改码前读 Host `pnc_rca_kafka_consumer.py` / `pnc_rca_outbox_dispatcher.py` / `pnc_rca_data_access.py` / `pnc_rca_schema.py`，以及 VM remote reader/service entrypoint
6. **纪律**：blocker 三类不互串；归因永远 need_review；输入只远程读；禁止 MDI、手工提交和 Agent fallback；VM 改动走隔离分支并留下 clean commit
