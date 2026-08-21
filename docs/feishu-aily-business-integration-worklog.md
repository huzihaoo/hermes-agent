# 飞书 Aily 业务接入设计工作记录

本文记录“业务知识如何无感接入 Hermes/RCA”方案的形成过程。它是本次设计工作的
有界 worklog，不是运行时流程，也不会要求每个业务任务重复执行这些阶段。

## 目标

让 Hermes 在识别到公司术语或业务任务时，像触发 Web Search 和本地知识检索一样
触发 Aily 企业知识问答。对 RCA，知识只帮助理解术语和聚焦分析方向，不能替代证据；
任何检索失败都继续原 RCA。

本轮按 owner 提供的方法完成设计，但这些步骤不进入运行时：

| 设计方法 | 本轮产物 |
| --- | --- |
| 盲区扫描 | 本文第 1 节的身份、grounding、幂等、host/VM 和 ACL 缺口。 |
| 先出原型 | 第 2 节的零副作用 shadow observer。 |
| 反向采访 | 第 3 节的三个架构问题及 owner 决定。 |
| 给参照物 | 第 4 节的真实代码、测试报告和 RCA 契约。 |
| 实施计划 | 第 6 节按反悔成本排序的实施顺序。 |
| Log 笔记 | 本文件，记录已确认、已否决和剩余缺口。 |
| 交接文档 | [集成交接](feishu-aily-business-integration-handoff.md)。 |
| 出题考你 | 交接中的 9 题证据 Quiz，必须 9/9。 |

## 1. 盲区扫描

2026-08-21 对候选 Aily 工具、gateway session identity、RCA outbox dispatcher、
execution request 和 VM 边界做了只读扫描，识别出以下会推翻方案的盲区：

1. 固定员工 UAT 依赖活跃飞书 session 的 `union_id`，不能用于 Kafka/outbox。
2. Agent Chat 的 `Completed + content` 不能证明调用了知识检索，也没有稳定来源契约。
3. Aily create 没有调用方幂等键；create 成功但响应丢失可能造成重复 conversation。
4. 当前 dispatcher 串行、带 lease 和 circuit；把最长 120 秒的查询 inline 会耦合故障。
5. 当前 `g1q3_rca_execution_request_v2` 没有一等业务参考或二阶段补查字段。
6. VM 分析后可能才发现内部术语，但 VM 不能携带 UAT 或直接联网。
7. 服务身份读取范围与最终 RCA 报告受众之间存在 ACL 扩散风险。
8. 共享 receipt 中低熵业务词不能只用裸 SHA，否则可被字典反推。

结论：关键词注册表不是首要难点。后台身份、只读 Agent、create-once、故障隔离和
host/VM 二阶段协议必须先于自动路由实现。

## 2. 最小原型

先提出一个无生产副作用的 shadow observer：

```text
completed RCA locator
  -> read-only observer
  -> deterministic business route
  -> bounded query builder
  -> dedicated read-only Aily provider or mock
  -> owner-only context + safe receipt
```

该原型不改 outbox、v2 execution request、VM、报告、投递或 production circuit。
停掉 observer 即完整回滚。它先回答四个问题：知识范围是否正确、返回是否有来源、
真实延迟/配额是多少、查询失败能否做到对原链零影响。

代码复核一度考虑在 dispatcher 内 enqueue，但进一步检查 outbox lease、fixed goal 和
delivery collector 后否决了该 seam：即使本地 enqueue 抛错也可能污染原 retry/circuit。
首版正式边界改为独立 observer 只读跟踪“VM task 已成功物化”，从 sealed goal 读取
最终 v2 request；不修改 dispatcher，也不在原链内等待 Aily。

## 3. 反向采访

只向 owner 提出了会改变架构的三个问题，2026-08-21 得到以下决定：

| 问题 | Owner 决定 | 方案影响 |
| --- | --- | --- |
| 无人值守 RCA 用什么身份？ | 按推荐：专用、最小权限服务身份。 | 交互 UAT 和 RCA provider 分离；固定员工 UAT 不进 resident/outbox/VM。 |
| 检索失败是否阻断 RCA？ | 不阻断。业务知识只帮助聚焦、术语和业务理解，失败继续原链。 | 删除 enforced/evidence-only gate；所有失败状态都必须保持原 VM/报告/投递行为。 |
| 首版是否处理分析中出现的新术语？ | 首版可以考虑支持。 | 首版契约预留 host-mediated gap/addendum artifact；主 RCA 不等待或 resume。 |

不再向 owner 询问可由代码、官方文档或安全边界直接裁决的问题。

## 4. 参照物

本设计使用以下参照物，不将任何一份摘要当 live execution truth：

| 参照物 | 用途 |
| --- | --- |
| `tools/feishu_aily_agent_tool.py` | 当前交互工具、Completed-only 状态机与身份边界。 |
| `tools/feishu_aily_agent_user_transport.py` | UAT broker、stdin、deadline、输出与进程隔离。 |
| `scripts/pnc_rca_outbox_dispatcher.py` | 真实 issue preread、identity validation、reservation 和 VM submit 顺序。 |
| `gateway/pnc_rca_schema.py` | v2 fixed execution request 和 evidence 边界。 |
| `gateway/pnc_rca_business_profiles.py` | 字段驱动的业务 profile/domain 参照。 |
| `docs/feishu-aily-agent-test-report.md` | 候选测试和 session-derived UAT canary 边界。 |
| `docs/pnc/g1q3-rca-intake-state-machine.md` | 当前 RCA host/VM 责任划分。 |

真实 canary 只能证明曾获得内部 OOI 相关文本，不能证明 no-public-web、逐答案来源或
grounding。正式实现必须重新形成 task-owned 安全 receipt。

## 5. 已锁定设计

- 普通任务：业务关键词/上下文命中后，在主模型前尝试预取；失败继续任务。
- RCA：任务类型无条件触发一次 required attempt；关键词只决定查询内容。
- 后台身份：专用服务用户/UAT，最小且经过批准的 RCA 知识范围。
- Agent：仅企业知识检索、无公网、无写工具和副作用 MCP。
- 影响：`observe_only` 或 `reference_only`，永远不是 execution evidence。
- 持久化：按 `(submission_key,generation,phase,query_hmac_sha256,
  provider_policy_fingerprint,identity_policy_fingerprint)` create-once；相同 query 只在
  同一 phase、同一授权主体/范围内去重。
- 二阶段：VM 只产生结构化 gap artifact，host 查询并写 owner-only addendum；主 task
  不等待、不 resume，失败继续原链。
- 公网边界：内部含义未命中不能 fallback Web。

以下旧方向已被 owner 决定明确取代：

- `business=required` 失败后阻断 VM；
- 因 `answer_only/no_match/error` 暂缓 RCA 投递；
- 把八步方案梳理方法写成每次任务的运行时状态机。

## 6. 风险优先计划

1. 封存原 RCA 零增强基线 oracle：request bytes/hash、goal hash、result、outbox 和投递。
2. 先证伪专用 service identity、知识范围、ACL 和无公网/无写工具 Agent 配置。
3. 定义 create-once 状态机和 owner-only/safe receipt，不确定 create 不盲目重建。
4. 上独立 shadow observer，测命中、误触发、延迟、配额和零影响。
5. 接普通飞书自动路由，失败保持原对话路径。
6. 在正式 RCA 分支实现只读 live observer + reference adjunct，保持 dispatcher、v2
   原契约、core result 和 required delivery 不变。
7. 完成 gap/addendum exact schema 和故障注入后，打开一次二阶段 feature flag；不
   增加主 task resume 状态。
8. 最后才做受管 materialize、gateway restart、readback 和真实 canary。

## 7. 当前验证与缺口

已验证：

- 候选 Aily user transport、tool、smoke、session identity 和 CLI 配置测试已通过；详情见
  测试报告。
- 固定 v2 request、dispatcher lease、VM fixed goal 和 immediate delivery 边界已经代码
  核对；因此明确排除 dispatcher enqueue 和 main-task resume。
- Owner 的三项架构决策已记录。

仍缺：

- 专用 RCA 服务身份和最小知识范围的真实 canary；
- Aily Agent 无公网/无写工具的可审发布 fingerprint；
- provider 活动/来源 receipt，或明确长期保持 `answer_only`；
- shadow observer、自动 router、create-once store 和二阶段协议实现；
- 正式 RCA 分支上的 fault matrix、零影响 oracle 和 production canary。

下一恢复入口是
[集成交接](feishu-aily-business-integration-handoff.md)，不是本聊天记录。

## 8. 证据 Quiz 结果

2026-08-21 对交接文档的 9 题做了设计合同自检：**9/9**。

| 题目 | 结论 | 合同证据 |
| --- | --- | --- |
| 1. Aily 超时是否阻断 RCA？ | 否。 | `failure_policy=continue_original_chain`，原 dispatcher/VM/delivery 均不变。 |
| 2. Aily 文本是否是根因证据？ | 否。 | `business_knowledge_is_execution_evidence=false`，只允许独立 reference。 |
| 3. 自动 RCA 用谁的身份？ | 专用最小权限服务身份。 | 固定员工 UAT 被排除在 resident/outbox/VM 之外。 |
| 4. 为什么先上 observer？ | 证明零影响并隔离 lease/circuit。 | observer 只读物化后的 task，不改 dispatcher/control DB。 |
| 5. create 响应不确定怎么办？ | `create_unknown`，不重复 POST。 | 后台 transport 必须 create 后先持久化 chat ID，再恢复 poll。 |
| 6. 新术语如何补查？ | VM 写有界 gap，host 查询。 | 主 task 不等待、不 resume；addendum 绑定同 generation/contract/S6。 |
| 7. 是否复用人工状态或原投递？ | 否。 | `human_blocking_state=false`、`required_delivery_effect=false`。 |
| 8. 开关增强后哪些基线不变？ | v2 request、goal、core、evidence 和原投递。 | `original_chain_mutations` 全为 `false`。 |
| 9. 什么允许从 shadow 晋级？ | 身份、只读 Agent、receipt、故障矩阵、零影响和容量证据。 | `Completed + content` 或本次自检分数都不够。 |

该 **9/9 只证明设计文档内部合同已闭合**，不代表 router/provider 已实现，不代表
grounding 已证明，也不代表 production release 或 resident canary 已通过。
