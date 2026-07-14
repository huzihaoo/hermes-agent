# G1Q3-RCA 自动分析全链路设计

状态：**remote-read v2 production candidate（2026-07-13，尚未部署）**

真相边界：本文定义候选契约，不证明 live 生效。生产状态只能由已部署 clean commit、resident process identity、Kafka/VM/delivery receipt 和 release gate 裁决。

## 1. 不可变边界

- 任务入口只有两类：Kafka Feishu issue creation 自动触发，以及固定受控业务群中、同时进入当前 active chat subset 的 `@小助手 + 明确动作 + 唯一 canonical issue identity` 人工触发。普通用户的分析/紧急分析映射为 `run_or_join`；`rerun/debug` 只对受控 operator 开放。
- 私聊、未 @ 的群消息、单纯 URL/状态/进展提问、裸 work-item ID 和 case ID 只读查询；不得创建任务或进入通用 Agent。
- 两个入口必须共享同一 source-neutral admission、create-once generation、durable outbox、固定 VM service 和 delivery store；群入口不得直连 VM、Agent 或旧 shared-state handoff。
- 问题数据只由固定版本 `pdcl_pyclip` 的 `RemoteEventReader` / `RemoteClipReader` 读取。
- 固定 `allow_download=false`、`input_materialization=forbidden`、`fallback=forbidden`；任何层级出现 MDI/PDCL download/refresh、输入下载额度或物化授权都 fail closed。
- Worker 固定 `direct_cli`、`agent_backend=none`；不得使用 Codex/Claude/LLM fallback 执行业务管线。
- 归因结论保持 `need_review`，系统不自动确认根因或责任人。

## 2. 双入口、单执行链路

```text
Kafka workflow event --------------------+
                                         |
受控群 @ + 明确动作 + 唯一 issue identity -+-> source-neutral exact admission
  -> durable trigger-source/binding ledger（Kafka 另有 raw-first inbox）
  -> create-once business trigger + immutable execution origin
  -> durable outbox
  -> Feishu issue fields/comments read
  -> normalized event/clip references
  -> derived-capacity reservation
  -> capability-scoped VM service
  -> attested direct worker process
  -> pdcl_pyclip remote read + completeness proof
  -> derived stream + seal
  -> governed S3a-S6 pipeline
  -> HTML manifest/contract + recursive dependency closure
  -> durable required delivery subscriptions/effects
  -> issue comment + 原始任务话题 marker read-before/write/read-after
  -> idempotent Feishu delivery（话题失败不得回退主群）
```

Kafka offset 只能在 raw event、classification、source/binding、business trigger/outbox 决策和 partition progress 同一 durable transaction 成功后推进。Kafka 或人工先创建 generation 的 source 成为 immutable execution origin；后到入口只作为 observer 绑定同一 generation。人工 `run_or_join` 永不创建重复执行；显式 `rerun/debug` 也只有在上一代执行与 required delivery 均真正终态后才创建下一代。Feishu message inbox 使用 `processing -> completed` 两阶段持久化状态；callback 抛错时放弃 processing 供重投，只有 durable admission 完成后才推进 API poll cursor。

## 3. Host 契约

### 3.1 双入口 admission

Topic、project key、project simple name、work-item type、status-change type、workflow transition 和 creation-rule version 都是 exact allowlist。每分区首次 offset 使用 owner 审定 T0；不存在 committed offset 且无 T0 时拒绝启动。

Feishu 事件中的 `project_key` 是 Meegle/API 内部项目键，用于读取工作项；`project_simple_name` 是浏览器 URL slug。候选 fixture 常见组合为内部键 `t03o4q` 与 slug `g1q3`，但上线值仍必须由真实 Kafka fixture/broker owner 裁决；两者不能互换或从显示名称猜测。

Consumer 与 outbox dispatcher 分离：

- safe-off 默认 `HERMES_RCA_KAFKA_SUBMIT_ENABLED=false`。最终候选配置在 preauthorization 前冻结为 `submit=true`，并令 Kafka、Outbox、Gateway manual、delivery collector、delivery dispatcher 五处 activation-required 均为字面量 `true`。物化原子窗口要求四个 RCA resident 与 Gateway 全停；物化后可立即启动 exact candidate Gateway 以保持其他机器人能力，但四个 RCA resident 在 preauthorization/preproduction 期间仍停止，且 Gateway 在“无 epoch / safe_off”下必须拒绝 RCA admission、保持 DB 无 pending/active effect，并将同一 PID/config/build 绑定到后续 canary。禁止再用 `submit=false` 形成无 epoch/ledger binding 的 shadow canary。
- `HERMES_RCA_OUTBOX_DISPATCH_ENABLED=false` 时不创建 VM task。
- outbox、derived capacity、VM lane、delivery backlog 或 resident health 任一过阈值即背压，不能靠跳过门禁追吞吐。
- 首次容量 bootstrap 仍显式使用 `resource_class=rca_prod`。BOM 覆盖的静态 runtime config 只记录预先确定的 release ID、bootstrap epoch 和容量模式；BOM/审批/授权哈希只进入 post-BOM active-release binding，避免固定点自引用。Dispatcher 每次 health/claim/submit 重读固定 binding、live env 和 owner-only authority，并要求三者的 release/epoch/BOM/approval/raw SHA/fingerprint 完全一致。
- candidate env 与 active-release binding 必须由同一 cutover journal 原子安装、共同快照和共同回滚。binding 缺失、过期、被替换或与 live env 字节不一致时，dispatcher 在 claim 前 fail closed。

Activation epoch 固定为 `(无 epoch) -> safe_off -> preauthorized -> bounded_active -> confirmed -> steady_active`，任一步均可在排空 pending/claimed writer 后 `aborted`。preauthorization gate 的不可覆盖 capsule 是 `create` 的唯一输入，且 `create` 只能建立 `safe_off`；operator 不能自行填写 fingerprint、DB identity 或 start fence。preproduction gate 在仍停写的 `safe_off` 上重验 preauthorization、config、typed DB identity、T0/start fence、冻结 canary plan 和迁移/物化连续性，产生第二个不可覆盖 capsule；只有 `transition-preauthorized` 消费它后，才能逐条授权 exact slots 并转 `bounded_active`。preauthorization、preproduction 与 production 各自都采用 `receipt + sibling capsule + sibling commit marker` 的逻辑原子发布；marker 最后写入且精确绑定前两者，缺 marker 的 capsule 不可消费，崩溃重试只允许从原 receipt 字节恢复缺失成员。epoch 同时绑定 preauthorization 与 preproduction 两组 fingerprint/gate-receipt/capsule hash，以及 build/config、DB logical identity、Kafka start/end fence、production gate receipt 和三类 exact canary budget。

业务 Activation 与容量 ratchet 是两个独立、单向的状态机。首次 release 的容量状态从 `BOOTSTRAP_PRODUCTION` generation 1 开始，即使业务已进入 `steady_active`，仍只允许并发 1、每日 5 次的 `rca_prod` 提交。进入 `bounded_active` 后、启动四个 resident 前，owner 必须执行 `prepare-bootstrap-production`：它消费同一 preproduction capsule，重验 exact slot authorization、active-release binding、live env、固定 bootstrap authority、owner 与 DB ratchet origin，在全局排他锁下 create-once 发布 `sample-producer-activation.json`。这一步只建立上线后的样本生产能力；20 个/7 天样本本身不作为首次 `production_bootstrap` 的前置条件。没有 producer receipt 时 runtime/release 仍 fail closed；`transition-steady` 不得晚建或替换它。

业务 `steady_active` 只表示 Kafka/群 @ 可以按 bootstrap 限额持续生产样本，不代表容量已放大。Host 必须积累至少 20 个、跨度至少 7 天、最大间隔/新鲜度合规且 `input_materialized_bytes=0` 的 v3 样本；容量 executor 随后以 create-once `steady-intent.json`、owner authorization、receipt、marker、evidence bundle 五件证据做 generation 1 -> 2 CAS。崩溃只允许 `recover --apply` 从已有 intent 中恢复，不读取 ambient operator/reason/authorization，也不自签 owner authority。容量 generation 2 成功只改变 admission capacity，不再次修改业务 Activation。

`canary_plan.json` 必须是 exact-shape `pnc_rca_canary_plan_v4`，固定 `admission_mode=direct_bounded`、`promotion_budget=0`、`slot_count=3`。槽位只能是 `kafka_success`、`manual_success`、`manual_terminal_failure`，每槽 `max_admissions=1`，入口分别为 `kafka_ingest/manual_admit/manual_admit`，结果分别为 `success/success/terminal_failed`；Kafka identity 绑定 exact event UID/topic/partition/offset，人工 identity 完整绑定 chat/requester/message/thread/canonical issue URL/`run_or_join`，三个 identity 与 submission 必须独立。plan raw SHA 在 preauthorization 冻结并由 preproduction 重验；`authorize` 和 `transition-bounded` 都必须消费同一 preproduction capsule，并在 resident 启动前逐槽比对 Store 授权。

bounded 只放行一个 Kafka success、一个正式群 manual success、一个原任务话题 terminal failure；只有 exact slots 已按冻结 plan 授权且 epoch 已转 bounded，才一次性启动四个 RCA resident，Gateway 若已按上述 safe candidate 启动则不得重启，之后到 steady 禁止更换配置/PID。确认前必须证明三条 ledger 已与真实 trigger/outbox 绑定且 outbox 完成，不能只消费空槽位。三条完成后 consumer 在同一进程自动暂停并回卷：broker group offset 为非负值时必须与 freeze position 完全相等；missing/`-1` 只在 freeze position 等于 owner T0 时允许，并回到 T0，绝不能伪造 commit。每分区 end fence 绑定 `source/offset/broker_group_offset/freeze_position/start_offset`。freeze session 固定 token/runtime/positions/paused-at，以独立 heartbeat `observed_at` 证明持续活性。Release Gate 将 stable freeze binding 写入不可覆盖的 confirmation capsule，激活 CLI 只能消费已由 commit marker 提交的 capsule，并在事务内重算 release binding。`confirm` 在 Kafka freeze 回读前后各重验一次 Gateway 与四个 resident 的完整 runtime continuity；任一 PID/create-time/boot/exe/cwd/cmdline/env/loaded-runtime/plist/launchctl 漂移均拒绝。confirmed 不放行任何 claim且继续保持 Kafka 冻结，只允许逐事件 reconcile 当前 epoch 且落在冻结 `[start,end)` 的 bound shadow；fence 外 Kafka 消息保持未提交并在 steady 后由同一 consumer 无重启重试。全局 shadow 未清零不得 steady。

替换 aborted epoch 前，旧 current epoch 的未执行 bound shadow/pending 必须用 exact event + operator/reason 审计 defer 为 quarantined；存在任一未处置 shadow 时拒绝创建新 epoch。defer 是显式恢复债务，不是成功终态，必须进入后续人工 `run_or_join` 或受控 backfill。所有 activation mutation 通过 `scripts/pnc_rca_activation.py --apply`；默认只读/plan，禁止 bulk、prefix 或 wildcard promotion。

生产 Store 只允许 open-existing：Kafka consumer、Outbox、delivery collector/dispatcher、Gateway manual admission 和 activation CLI 均以 `require_current=True` 打开绝对路径、owner 可控、regular、单链接、非空且精确 v10/v6 的 SQLite；缺库或旧 schema 不得由常驻服务自动创建/迁移。`<db>.pnc-rca-maintenance` 或 `<db>.pnc-rca-tombstone` 在启动、初始化后和每次 connect/write 前都 fail closed。marker 只能由同一受控 journal/receipt 恢复，禁止手删后重试。

人工入口独立 safe-off：

- `HERMES_RCA_MANUAL_INTAKE_ENABLED=false` 时明确动作也只返回安全关闭，不写 source/outbox/subscription。
- 总闸为 true 仍不足以放量；`HERMES_RCA_MANUAL_CHAT_IDS` 必须是两个固定群的非空子集。未知群、空集合或混入未知 ID 全部 fail closed。先只开放测试群并完成 canary，再显式加入生产群；授权 receipt 只记录排序集合的 SHA-256，且同一内存快照传入 control store，避免配置 TOCTOU。
- `rerun/debug` 必须同时通过 canonical `HERMES_RCA_MANUAL_OPERATOR_ENABLED`、`HERMES_RCA_MANUAL_OPERATOR_USER_IDS`、requester allowlist hash 和授权 receipt，并受 `HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT=3` / `HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS=600` 的 durable 事务限速；production 开启 manual intake 时 operator 开关必须显式配置，若为 true 则 allowlist 必须显式非空且两项限速必须是显式正整数。旧 `HERMES_RCA_MANUAL_DEBUG_*` 仅是 canonical 值缺失时的兼容 fallback，不是新部署配置。普通分析/紧急分析只能 `run_or_join`，仍受固定群、真实 @、唯一 canonical issue identity 和 L1 输出面约束。
- URL 可来自当前消息、结构化卡片链接元数据，或被回复卡片正文，但回复内容只能补 issue identity，不能提供 `分析/rerun/debug` 执行动词。出现多个不同 issue identity 时拒绝创建；同一 canonical URL 重复出现不算多单。
- 每条人工 source 都与 generation、原话题 subscription 建立多对多审计绑定；同一话题重复 @ 只产生一个 delivery effect，不丢失各自 source/授权证据。
- Kafka-first/manual-later 保留 Kafka origin；manual-first/Kafka-later 保留 manual origin。observer 不得覆盖 outbox payload 或 execution request source。

### 3.2 Issue read

Meegle 是主读取源，受界 MCP 只读 auto-degrade 是可观测降级。execution evidence 必须记录读取 source、degraded 标志和脱敏错误类别；读取失败、字段缺失和数据访问失败是不同 blocker。

Issue preread 默认无写副作用。旧 `issue_capture` 仅允许显式诊断 opt-in 且根目录必须位于 `/mnt/tmp/<task>/`；production release 禁止开启。

### 3.3 Remote data ABI

`data.data_access` 使用 exact-shape `g1q3_rca_remote_data_access_v1`：

- `mode=remote_read`
- `transport=pdcl_pyclip`
- reference 为 `event_uuid + RemoteEventReader` 或 `clip_uuid + RemoteClipReader`
- source 绑定问题字段名和原始地址信封 SHA-256，但原始命令不传 VM
- reader distribution/version 固定，完整范围读取，禁止 fallback/MDI

Host 在提交前递归检查 dataclass/dict/list 的原始和序列化请求，拒绝未知下载字段、真值下载 flag、非零下载额度、MDI/PDCL 命令、错误 data mode 和输入物化。

## 4. Worker 与 VM 契约

- Host 只能调用固定 `vm_task_submit_service` capability；公共 `vm_task_submit` 拒绝 RCA namespace、service markers、artifact path 和 Feishu issue URL。
- Worker 在 `Popen` 成功后原子写 attestation，绑定 task ID、worker run ID、PID、argv、cwd、dispatch receipt hash 和入口 hash；service 必须验证同一 PID/run/argv/cwd。
- VM service 只接受 `g1q3_rca_execution_request_v2`，自身再次做 exact no-MDI 校验；v1/manual/download CLI 在任何文件、进程或网络副作用前拒绝。
- 固定 `/usr/bin/python3` + hash-locked repo-local overlay。每单执行前验证 dependency version、resolved source、RECORD/文件集 hash，拒绝外部 `PYTHONPATH`/`PYTHONHOME` 和 runtime fallback。
- Remote read 必须证明 requested scope、topics/channels、time window、message/scan/output limits、reader exhaustion 和完整性；不能静默截断。
- 允许写入的是任务隔离的请求、派生流、seal、stage receipt、报告和交付证据，不允许写 production canonical cases root。

## 5. MCAP 与资源隔离

转换只能由 task-owned governed execution 启动：

- image 固定 registry/name + `sha256` digest，tag-only 拒绝
- memory、CPU、PID、timeout 显式上限
- rootfs read-only、private IPC、network none、最小 bind mount
- labels 与 `ssh-mini-mcap-run` / watchdog / reaper governed contract 一致
- task/run/container ownership 和退出 cleanup receipt 完整
- build/cache/intermediate 位于 `/mnt/tmp/<submission_key>/`，不污染源码树

若 SIGKILL 后 watchdog/reaper 不能安全识别并回收任务容器，则 production NO-GO。

## 6. 存储与交付

- VM 工作根固定 `/mnt/tmp/<submission_key>/`。
- 用户可见路径固定 `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<submission_key>/`。
- `g1q3_rca_storage_admission_v2` 只对 `/mnt/tmp` 这一物理池做一次只读 `statvfs`；输入物化固定为 `0/forbidden`。每 case 的逻辑预算为派生缓存 `1.0x` 加分析/发布产物 `2.25x`，合计 `3.25x` 后再与并发量、30% reserve 和目标 cases/day 计算准入/horizon。旧 reservation ledger 的 `tmp/hfs` 列仅表示逻辑预算拆分，不代表第二个 HFS 物理容量目标。
- 每单先做派生产物 atomic reservation，stage 记录 observed/delta/peak bytes，终态按策略 release；输入规模不转换成下载预算。
- remote cache、request storage、service result 三处 CIFS mount evidence 必须完全一致，包括 mount source、fstype、mode、rw、device ID 和 mount namespace。
- HTML 必须由 manifest + contract 证明所有 CSS/JS/media 依赖闭合、无外链/路径逃逸，HTTP read-after-write 成功，并通过 Chromium desktop/mobile smoke。
- Feishu 写入只来自 durable delivery effect；Kafka generation 至少有 required issue-comment subscription，人工 source 还必须有 required origin-topic subscription。
- late join 在报告已生成后也要原子补建 topic effect；issue/comment 与 thread-reply 独立 claim、lease、retry 和 circuit，一个边界故障不能阻断另一个已排队 effect。
- thread effect 使用 effect-key 派生的稳定 Feishu UUID，并在原话题分页读取 marker 做写前/写后对账；话题不可见、根消息不匹配或 reply 被拒时 fail closed，禁止改投主群。
- 成功 HTML 与 `terminal_failed`/quarantine 均已进入 durable 双交付候选实现。包括 VM 提交前 quarantine：当 outbox 已隔离但尚无 watch 时，同一事务创建 terminal watch、quarantined outcome job、required issue-comment effect 和所有人工原话题 effect；公开错误码固定为 `outbox_submission_quarantined`，内部 code/detail 只留 DB，不伪造 HTML/report。pending/uncertain effect 完成前禁止 rerun/debug。
- dispatcher 复用稳定 marker/UUID、写前/写后对账和双 circuit，话题失败不得回退主群。5/15/60 分钟 delivery outcome SLO 或 1 小时内连续 3 个终态交付失败会背压新 outbox，但不阻断既有 delivery recovery。真实飞书成功/失败 canary 完成前仍是 production NO-GO，不能用单测或只读查询替代主动终态通知证明。

首次缺库时的 `fresh_install` 只物化 control/delivery SQLite 控制面，不读取、下载或落地任何问题输入、event、clip 或 MCAP；业务数据的 `input_materialization=forbidden` 永远不变。

## 7. 性能与容量目标

初始目标以 200 case/day 设计，但上线资格来自测量而非静态估算：

- `production_bootstrap` 首次上线前以最近 7 天真实 Kafka 消息的 bounded replay 取代 24 小时 soak 硬门槛：显式时间戳 offset/fixed end offset、`group_id=None`、无 subscribe/join/commit，至少 1 条真实消息通过当前 policy 并创建 shadow trigger/outbox。24 小时、至少 200 个代表 case remote-reader soak 改为上线后观察项；正式证据格式仍固定为 `pnc_rca_remote_reader_soak_v4`，供后续具体问题分析或 steady-capacity 放大使用
- soak 不能以长时间 idle 冒充 24 小时运行。24 个一小时 bucket 每桶至少 4 单，首末 bucket 必须覆盖、相邻启动最大间隔 3630 秒；另需至少 900 秒处于并发 4、最长连续并发至少 300 秒。每 case 的 wall-clock 与 monotonic offset 交叉校验，120 秒边界不因调度误差放宽
- soak 的临时 stream cache 只允许存在于 task-owned `/mnt/tmp/<task_id>/`，remote receipt/completeness/hash 验证后立即 unlink+fsync；正式 evidence 要求 `retained_stream_cache_bytes=0`。因此 750 MB 是单 case/并发峰值硬边界，不是允许累计保留 150 GB
- success/error/timeout/limit 分类完整
- p50/p95/p99 issue read、remote read、stage、end-to-end 和 delivery latency
- Kafka lag/outbox depth、VM lane utilization、memory/swap/load、derived bytes/day、retention horizon、delivery backlog/circuit
- 人工新执行最多占共享 active outbox 水位的 80%；同问题 `run_or_join` 只 join、不新增 backlog。dispatcher 在 Kafka 与人工均有等待时采用 3:1 有界公平调度，既为 Kafka 自动入口保留容量，也保证群内紧急请求不会被持续 Kafka 流量饿死
- 四个 resident 的 health heartbeat 不依赖整批完成；Outbox 每 10 秒刷新 liveness，delivery 服务处理外部边界期间最多每 15 秒刷新。业务 readiness、circuit 与错误状态仍独立判定，心跳不能把失败伪装成 ready
- issue preread 总预算 75 秒：Meegle 每次最多 12 秒，MCP 每次最多 15 秒；同步 MCP proxy 也必须可中断并取消后台 RPC。超时形成 `host_issue_preread_timeout` durable retry，不能被解释为字段缺失
- Feishu SDK/话题单次读写 deadline 固定 12 秒；写超时一律视为 outcome uncertain，并通过稳定 marker/UUID 对账。delivery effect claim 强制启用独立 lease keeper，每 10 秒按原 token/fence/owner 续租，最大允许间隔 15 秒；成功、重试、隔离和熔断结算都必须在 keeper 锁内先同步续租，续租失败时旧 worker 禁止提交本地成功态。keeper 保护 claim 所有权但不能杀死仍在 OS 线程运行的 SDK 调用，晚返回仍按 uncertain 对账，不能盲重发。delivery bundle 保留 512 MiB/512 文件硬上限，Host 读取 deadline 最多 110 秒且必须比 lease 至少短 15 秒
- steady consumer 每次 poll 只读取 indexed current epoch；完整 raw retention/capacity health 只按监控节奏采集。bounded 短窗口才计算 exact slot、bound execution、pending inbox 与 inflight writer readiness，禁止把完整 health 扫描放进 steady 热路径
- 长期 health/SLO 查询使用 source/status、job updated-at 与 attempt outcome/time 覆盖索引；元数据归档必须另走 receipt-backed 专项，不允许为了性能自动永久删除历史审计记录
- 单 case 120 秒 remote-read timeout；全任务最大执行预算与 release receipt freshness 分开校验

任何容量 horizon、error rate、backlog、latency 或 VM pressure 超硬阈值时停止新 dispatch，不牺牲完整性或资源隔离。

## 8. 发布与回滚

上线前必须同时具备：

1. broker metadata、精确 topic casing、partition、T0 和真实 creation fixture；当前 live `.env`/候选使用全小写 `feishu-project-workflow-event`，而外部交接字符串曾出现 `feishu-project-workfLow-event`，Kafka 大小写敏感，必须以 broker metadata 和 owner 确认为准
2. Host/worker/VM clean commit、critical BOM、resident runtime identity
3. 固定 remote-reader overlay 和完整 dependency proof
4. pinned MCAP digest 与 governed cleanup proof
5. 最近 7 天真实 Kafka bounded replay、fresh capacity/retention horizon；remote-reader soak 作为上线后观察项
6. `bounded_active` 下 exact `kafka_success` slot 直接 admission 的真实 governed canary；preauthorized shadow/promotion 不得替代
7. collector 只读生成、由 `canary_receipt_commit.json` 原子指向的不可变 receipt + sources generation pair
8. release gate、browser smoke、成功双交付 read-after-write，以及 terminal-failure 原话题 durable 回执 canary 全绿；终态 canary 必须绑定配置中的真实 control/delivery SQLite path + device/inode，并证明 source-created、terminal、materialized、completed、collected 时间线单调且整链新鲜
9. release gate 必须按 live 事实选择且只选择一条 SQLite 路线：已有 exact v8/v5 时，生成一致性备份并由同 commit/BOM、`100755`、单链接的 predecessor validator 以真实 subprocess 只读验证恢复库；缺少 validator 即 NO-GO。configured DB 真实缺失时，migration v3 只能生成 seed 并声明 `fresh_install_materialization_required`，随后由显式、可恢复的 materializer 建立带 genesis/journal/receipt 的 v10/v6 数据库。`already_current` 及 DB 内 genesis/origin meta 只能证明内部连续性，不能自证可信来源或补做 rollback evidence；生产只能继续消费原始、完整且可重验的 materialization receipt/journal，或重新走真实 predecessor restore 路线。两条路线都要求 resident health 精确报告 `pnc_rca_control_store_v10` / `pnc_rca_delivery_store_v6`，旧 binary 不能直接读取新 schema
10. delivery dispatcher candidate plist 与安装后 plist 的 `ProgramArguments[0]` 精确解释器依赖证明；必须在该解释器内验证 pinned `lark-oapi==1.5.3`、`ReplyMessageRequest`、`ReplyMessageRequestBody` 及 `reply_in_thread` builder API，不能用交互 shell 的 Python 或仅检查 lockfile 代替
11. 若启用了 Feishu API poll 补偿入口，回滚前必须验证 `~/.hermes/feishu_api_poll_state_v1.json` 的 `rollback_readiness.ready=true`、`rollback_readiness.pending_count=0`、`rollback_readiness.scan_continuation_count=0`；文件存在时必须跨二进制回滚保留，旧 binary 可以忽略但不得删除。文件缺失只能由“该实例从未启用 API poll”的配置与运行证据解释，不能直接视为已排空

回滚顺序：先关闭人工 intake 与 outbox dispatch，再关闭 Kafka submit，停止新任务并等待/隔离在途 generation；保留只读状态查询。人工 intake 关闭前先等待 API poll ownership 排空并固化上述 sidecar evidence。已有 predecessor 路线必须停写并恢复已验证快照后才能启动旧 binary。greenfield 路线不存在可恢复的旧 live DB，只能关闭五个 writer、保留当前 v10/v6 DB 与 materialization receipt/journal，并在替换前通过受控 quarantine/tombstone 操作隔离；禁止删除、降级或让旧 binary 打开它。正式的受控群 `@小助手` 入口是必须保留的产品能力；这里永久禁止恢复的是绕过 durable outbox/control store 的旧群聊直提 VM、公共 VM task、Agent、MDI 下载或旧 production cases-root 写入。

## 9. 历史资料

2026-06-11 至 2026-06-12 的 MDI 下载方案、case 验收和旧容量数字只在 Git 历史及 `/Users/songying/Documents/G1Q3_RCA_local_business_knowledge_handoff_2026-07-10.md` 中保留。该 handoff 明确把 remote reader 评为当时尚未接入的候选能力；本轮实现和验证负责把它升级为 production candidate，但在 soak/canary/release gate 完成前仍不能声称已生产可用。
