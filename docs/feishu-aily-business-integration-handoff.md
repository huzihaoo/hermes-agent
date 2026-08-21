# 飞书 Aily 业务知识接入交接

## 当前状态

- 交互式 Aily Agent/UAT 候选已完成；生产 gateway 尚未启用。
- 自动业务路由、RCA provider、shadow observer 和二阶段补查尚未实现。
- 当前候选分支与 live RCA 已分叉，不能整体 materialize 或直接合入生产。
- Owner 已批准设计方向，但仍要求等待正式 RCA 分支后再移植和发布。

设计正文见 [业务知识检索接入设计](hermes-knowledge-retrieval-routing.md)，形成过程见
[设计工作记录](feishu-aily-business-integration-worklog.md)，已有测试证据见
[Aily Agent 测试报告](feishu-aily-agent-test-report.md)。

## 不可变决策

1. 自动 RCA 使用专用、最小权限、只读服务身份；固定员工 UAT 只用于本人交互路径。
2. Business lookup 是增强项。任何失败、超时、未命中或 `answer_only` 都继续原 RCA。
3. Aily 内容是 reference knowledge，不是 issue/MCAP/log execution evidence。
4. 二阶段补查只能由 VM 发 gap、host 查询；VM 不持有凭据或直接访问 Aily。
5. 内部术语未命中不能 fallback Web。
6. 设计所用八步方法不进入 runtime contract。

## 移植边界

候选历史至少包含：

```text
a1e4565ec7 -> e25ba59684 -> 4a0adaba91 -> dda09b7042
             -> 4b0623221d -> 70b15406e0 -> 368e3a2176
```

不要直接 cherry-pick 整条历史。正式 RCA 分支就绪后按 Aily 文件/hunk manifest 人工
移植并复核共享文件，尤其是：

- `gateway/session_context.py`
- `gateway/run.py`
- `hermes_cli/tools_config.py`
- `plugins/platforms/feishu/*`
- `tools/feishu_aily_agent_*.py`
- Aily 相关测试和本文档

RCA 集成应新建 typed provider 边界，不要直接从 dispatcher 调用模型工具 handler。
推荐新模块职责：

```text
gateway/business_knowledge/routing.py       deterministic route + registry
gateway/business_knowledge/query.py         allowlisted query builder
gateway/business_knowledge/provider.py      typed provider protocol/result
gateway/business_knowledge/store.py         create-once context/receipt
scripts/pnc_rca_knowledge_shadow.py          zero-impact observer
```

实际命名可遵循正式分支现有模式，但边界不能合并成一个 dispatcher 网络函数。

## 分阶段实施

### R0：原链 oracle

对固定 RCA fixtures 封存以下基线：

- v2 execution request bytes 和 canonical hash；
- fixed VM goal hash；
- evaluator/evidence、L0/L1 和最终结果；
- outbox 状态、retry/circuit/quarantine；
- required delivery effects。

所有增强故障测试都必须证明这些基线不变。

### R1：服务身份与 Agent 策略

- 创建专用服务用户/UAT，不使用固定员工 profile。
- 仅授权适合 RCA 报告受众的知识范围。
- Agent 关闭公网、写工具和副作用 MCP。
- 保存发布版本、知识范围和工具策略 fingerprint。
- 验证凭据轮换、到期和撤权，不把身份信息写进 VM 或 shared receipt。

### R2：Shadow observer

- 只读 completed RCA 定位符，不持有 RCA outbox lease。
- 写 owner-only `business_knowledge_context_v1` 和 safe receipt。
- provider 故障使用独立 metrics/circuit，不影响原 dispatcher。
- 测量触发率、命中率、误触发、p50/p95、配额和 helpfulness。

### R3：普通任务自动路由

- session identity 绑定后、首轮模型调用前执行 deterministic route。
- 命中业务规则时预取一条有界 query。
- 失败继续原任务，内部含义不 fallback Web。

### R4：RCA live reference lane

- 独立 observer 只读跟踪已成功物化的新 VM task，从 sealed goal 解析最终 v2 request。
- 不修改 dispatcher、不持 outbox lease、不写原 control DB；网络查询使用独立 durable
  coordinator/store。
- 结果只作为 owner-only reference adjunct；不写 `RcaExecutionRequest.evidence`，
  不改变 evaluator、sealed core result、原报告或 required delivery。
- `(submission_key,generation,phase,query_hmac_sha256,
  provider_policy_fingerprint,identity_policy_fingerprint)` create-once；身份或知识 ACL
  fingerprint 变化不得复用旧 job。

### R5：二阶段补查

- Exact schema：`business_knowledge_gap_v1` 和 `business_knowledge_addendum_v1`。
- 首版最多一轮、两条 query；host 重构问题，不能原样执行 VM 文本。
- Gap 固定写入 `<artifact_root>/business_knowledge/gaps/gap-1.json`，绑定 task meta 的
  `rca_contract_sha256` 与 `stage_lineage/s6_report.json` 的 canonical SHA-256，且绝不进入
  delivery contract/manifest。
- Addendum 绑定同一 generation/contract/S6/lookup-receipt hash，但主 task 不等待、不
  resume，也不复用
  human `need_input` 状态。
- late/invalid gap 或 error lookup receipt 只终止 reference lane，不创建 failure addendum，
  不重写已完成结论或原投递。

### R6：发布

- 等正式 RCA production branch，只移植 Aily/provider 相关 hunk。
- 运行 Aily focused、RCA contract、dispatcher fault、VM fixed CLI 和 delivery suites。
- 经 governed materialize、manifest/config/env fingerprint、gateway restart 和 readback。
- 先 shadow，再 bounded canary；feature gate 默认关闭。

## 必测矩阵

1. `task_type=rca` 无关键词仍尝试 business；普通内部词触发，通用问题不触发。
2. Query allowlist 不含 ID/URL/PDCL/frame/用户/评论/附件/完整描述/root cause。
3. 员工 UAT 在 Kafka/outbox 路径发请求前失败；service identity 独立且最小权限。
   后台 create 必须省略 `session_id`，每个 job 使用全新 session，不跨 task/generation
   复用对话历史。
4. `Completed + content` 无来源只能是 `answer_only`。
5. timeout、403、429、5xx、no-match、bad JSON、oversize、crash 全部继续原 RCA。
6. 增强故障不触发 dispatcher retry/circuit/quarantine，不延迟 VM submit 或 delivery。
7. Aily 文本不进入 evidence、evaluator、causal chain、responsibility 或 quality gate。
8. create 前后和 poll/receipt 各 crash point 不允许客户端发第二次 POST；模糊 create
   可能留下服务端孤儿 chat，只能记录 `create_unknown`；后台 transport 必须支持
   create 后先持久化 chat ID 再 poll，不能直接复用当前一体化 user transport。
9. safe receipt 不含 query/answer/user/chat/session/token；低熵 query 使用 HMAC。
   Lookup receipt 字段必须严格匹配 status schema，任何额外正文/raw provider payload
   都拒绝。
10. VM 只写原子封存且不进入 delivery manifest 的 gap artifact；host 才访问 Aily；
    context/addendum 必须绑定通过条件 schema 校验的 immutable lookup receipt，同 hash
    不循环，主 task 不等待或 resume。
    gap absent、ENOSPC、permission、schema/hash、no-replace conflict 或 fsync 失败都只
    跳过 reference branch，不改变 fixed CLI exit、原 result/report 或 required delivery。
11. 禁用 feature gate 后，request bytes/hash、result 和 effects 与 R0 基线一致。

## 证据 Quiz

提交实现或申请发布前必须 9/9；每题答案需要代码、测试或 receipt 定位符，不能只靠
模型自评。

1. **Aily timeout 是否会阻断 VM、重试 outbox 或停止投递？**
   - 满分：都不会；记录增强失败并继续原 RCA。
2. **Aily 回答能否直接成为根因、定责证据或提高证据置信度？**
   - 满分：不能；只作 reference，必须由原始数据/代码/live 证据独立验证。
3. **自动 RCA 使用什么身份，能否借固定员工 UAT？**
   - 满分：专用最小权限服务身份；不能借员工 UAT。
4. **为什么先上独立 observer，而不是直接在 dispatcher inline 查询？**
   - 满分：先证明零影响，避免把延迟、lease、配额和 circuit 耦合进原链。
5. **Create 成功但 chat ID 未知时怎么处理？**
   - 满分：记 `create_unknown`，不盲目重复 create，返回空增强并继续 RCA。
6. **VM 新发现术语后由谁查询，主任务是否恢复？**
   - 满分：VM 发有界 gap artifact 后继续原任务；host 查询并封存 owner-only addendum，
     绑定原 generation，但主 task 不等待、不 resume。
7. **二阶段 gap 能否复用人工 `need_input`/`pending_confirm` 或 required delivery？**
   - 满分：都不能；它是独立 reference lane，失败不得触发人工工作流或原投递副作用。
8. **增强开启/关闭时哪些原链基线必须不变？**
   - 满分：v2 request/hash、goal、evaluator/evidence、结论、outbox 和 required delivery。
9. **什么证据允许从 shadow 晋级？**
   - 满分：真实 service identity、只读 Agent 配置证明、create-once fault matrix、
     无泄漏、原链零影响、二阶段 host-only、预算和延迟/配额可接受；非空文本不够。

## 回滚

- Shadow：停止 observer。
- 自动 route/preflight/二阶段：关闭独立 feature gate。
- 回滚不得要求修改原 RCA DB、回退 production branch 或借用旧员工 UAT。
- 若关闭 gate 后原链不能恢复 R0 基线，说明实现违反“纯增强”，不得发布。
