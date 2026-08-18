# G1Q3-RCA 自动分析链路

状态：当前架构与生产合同。

本文描述产品边界和发布验收条件，不声明某个版本已经在线。当前状态必须以 live manifest、ControlStore、resident 进程/health、VM task receipt 和飞书字段读回为准。

## 1. 产品边界

- Host 是控制面：读取飞书问题单、做准入和幂等、创建 durable outbox、投递 VM、收敛结果并完成飞书外部交付。
- VM 是 RCA 计算执行面：scheduler、实时资源 gate、receipt/lease、固定 CLI、结果回传。VM 不负责正式飞书写回。
- VM 业务仓承载 RCA 领域实现和报告生成。生产任务只能运行 release note 固定的 immutable runtime。
- Codex、Claude 和通用 Agent 都不是生产 RCA 分析执行器。正式路由固定 `direct_cli`、`agent_backend=none`，禁止 Agent fallback。
- 问题数据只经固定版本的 `pdcl_pyclip` remote reader 读取；固定 `allow_download=false`、`input_materialization=forbidden`，不得恢复 MDI 下载或本机输入物化。
- 归因结论保持 `need_review`。系统可以生成候选归因，但不能自动确认根因或责任人。

## 2. 业务链路

```text
operator issue-only / 受控业务入口
  -> source-neutral exact admission
  -> create-once generation + durable outbox
  -> 飞书 issue 字段/评论读取与 remote reference 归一化
  -> steady rca_prod 实时资源准入
  -> VM fixed direct CLI
  -> remote read + completeness proof
  -> S2/S3a/S3b/S5/S6 + HTML report
  -> delivery collector
  -> durable field effects
  -> 飞书 read-before / write / read-after
  -> transport acceptance
```

同一 canonical issue generation 只能执行一次。已有 generation 未进入真实终态时，`run_or_join` 只能加入观察；重试必须创建 `generation + 1`，不得复活旧 task、submission key 或 delivery effect。

## 3. Host 合同

### 3.1 准入与状态

- Issue identity、项目、工作项类型和允许动作均使用 exact allowlist。
- 生产 Store 只允许 open-existing；常驻进程不得自动创建或迁移生产 SQLite。
- Outbox、delivery effect、provider marker 和 read-after-write 状态都必须 durable。进程退出或网络结果不确定时，不得从内存状态推断成功。
- 新任务固定使用 `resource_class=rca_prod` 与 `capacity_mode=steady`。静态 release identity 不能替代任务开始时的实时资源检查。
- 排队时间不计入 VM 执行超时；执行预算从 worker 实际启动进程开始计算。
- 计算终态和外部交付终态分别记录。报告存在、任务 completed 或 collector healthy 都不能替代飞书字段读回。

### 3.2 Issue 读取

Meegle 是主读取源。`workitem get --fields _all` 必须遍历全部分页后才发布合并载荷；分页失败、token 循环、重复或超限一律 fail closed，不能把截断载荷解释为字段缺失。

Issue preread 默认无写副作用。读取失败、字段确实缺失和数据引用格式不合法是三个不同 blocker，必须保留独立错误码。

### 3.3 Remote data ABI

`data.data_access` 使用 exact-shape `g1q3_rca_remote_data_access_v1`：

- `mode=remote_read`
- `transport=pdcl_pyclip`
- reference 为 `event_uuid + RemoteEventReader` 或 `clip_uuid + RemoteClipReader`
- source 绑定问题字段名和原始地址信封 SHA-256，但不把原始命令传给 VM
- 完整范围读取，禁止 fallback、下载和输入物化

Host 与 VM 都要递归拒绝未知下载字段、真值下载 flag、非零下载额度和错误 data mode。

## 4. VM 合同

- Host 只能调用 RCA 专用 service ingress；公共 VM task 入口拒绝 RCA namespace 和 service marker。
- VM service 只接受固定版本 execution request，并在任何文件、进程或网络副作用前再次做 no-download/no-MDI 校验。
- Worker 必须记录 task ID、worker run ID、PID、argv、cwd、dispatch receipt hash 和固定 CLI identity。
- 每单执行前验证业务 runtime commit/tree、依赖版本和 fixed CLI；不得从 mutable branch 或 ambient `PYTHONPATH` 选择实现。
- 任务工作根固定 `/mnt/tmp/<submission_key>/`，用户可见路径为 `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<submission_key>/`。
- MCAP/转换仍使用既有受管 VM 执行器及实时资源限制；release 工具不复制这些能力。

## 5. 交付合同

成功任务必须通过 durable delivery effect 写入：

1. 非空「归因结果」字段。
2. 可访问的正式 HTML「归因报告」字段。
3. 每个字段各自的 read-before、write、read-after。
4. read-after 与本 generation 期望值完全一致后，effect 才能 completed。

Provider 超时或返回不确定时，使用稳定 marker/UUID 和字段读回裁决，禁止盲目重写。技术执行失败不计入飞书 provider circuit；只有真实 required field effect 的 provider 失败才影响该 circuit。

## 6. 最小生产发布合同

Host 生产发布只有一条权威链路：

```text
GitLab exact branch/tag/commit/tree
  + owner-only minimal release note
  + immutable Host/worker/pipeline runtime identity
  + atomic ControlStore v14-to-v15 migration and steady successor
  + operator_issue_only_v1 restart/readback
  + one transport canary
```

生产 Git remote 固定为：

```text
git@git.minieye.tech:planning_algo/hermes.git
```

GitHub 只保留 Hermes 官方源码的 upstream/reference 语义，不是 RCA production remote，也不要求与官方仓同步。

Minimal release note 必须固定：

- release ID 和生产定义；
- Host、worker、pipeline、report service 的 exact identity；
- Host GitLab branch、tag、commit、tree 和 tag object；
- immutable runtime 绝对路径；
- live manifest 与 env SHA-256；
- v14 predecessor、target v15 schema、ControlStore identity、partition fence 和 release note 内嵌 epoch contract；
- `operator_issue_only_v1` resident profile；
- 唯一 canary issue 和 state path。

发布由 `scripts/pnc_rca_minimal_release.py` 执行：

1. `plan` 只读验证 GitLab refs、release note、候选 manifest/env、immutable runtime、exact v14 predecessor/audit/inflight/fence 和 resident 前态。
2. `apply` 先停止六个 resident，以 exact hash 安装 manifest/env，再通过 `RcaControlStore.migrate_v14_to_v15_and_activate` 在一个事务内重建 activation schema、CAS marker 并激活唯一 v15 steady successor；只有 outcome 独立读回为 `committed` 后才重启四个 required resident。
3. 单独提交一个 `--acceptance-axis transport` 的 operator issue-only canary。发布工具自身不创建额外业务任务。
4. `verify` 必须读取本次 completed apply receipt，并同时通过 GitLab/runtime identity、live projection、steady epoch、resident PID/readback 和单 canary。

`operator_issue_only_v1` 的四个必须运行面为：

- `ai.hermes.gateway`
- `local.pnc.rca-outbox-dispatcher`
- `local.pnc.rca-delivery-collector`
- `local.pnc.rca-delivery-dispatcher`

该 profile 明确禁用：

- `local.pnc.rca-kafka-consumer`
- `local.pnc.completion-notice-relay`

Resident 验收必须读取新 PID、实际 cwd/entrypoint、runtime identity 和新鲜 health；health 文件存在但进程不在、PID 不符或仍绑定前一 release 都不算通过。

Canary 的 transport 轴要求非空正式 comment ID、exact 字段集合 `field_8c912e`/`field_9193cb`，并且只接受官方写后读回来源 `read_after_write` 或 `read_after_recovery_write`。Causal attribution 作为同一 state 中的独立诊断结果呈现，但不阻塞 Host transport release；归因能力验收由其业务负责人独立推进。

旧多阶段发布工具、中间证明文件和容量爬坡发布流程已经退役，不得作为旁路恢复。v15 迁移提交前，只有 outcome=`not_committed` 才允许恢复 artifact；提交后只允许 successor-read-only 或 forward fix，outcome=`unknown` 必须保持所有 resident 停止并人工裁决。禁止原地改 production runtime、手工拼绑定或直接写 SQLite。当前 minimal driver 是一次性 v14-to-v15 cutover driver；后续 v15 release 需要另行审定的新合同，不能复用本次 note 创建新 epoch。

## 7. 生产验收

稳定生产至少证明：

1. GitLab branch/tag 均解析到 release note 固定的 Host commit，tree 与 immutable runtime 一致。
2. Worker、pipeline 和 report service identity 与 release note 一致，生产任务的 route/cwd/fixed CLI 无漂移。
3. 四个 required resident 均为新 PID，cwd 指向 immutable Host runtime，health 新鲜且 release block 完整。
4. 两个 disabled resident 未加载。
5. ControlStore current epoch 为 release note 内嵌 activation binding 固定的 `steady_active` epoch。
6. 唯一 canary 的当前 generation 已 completed，transport PASS，两个飞书字段完成官方读回。

单测、报告文件、shared-state completed 或因果结论本身都不能替代上述 transport 证据。详细命令见 [生产 Runbook](g1q3-rca-ops-runbook.md)。
