# 飞书 Aily 业务接入设计工作记录

本文记录“业务知识如何无感接入本机 Hermes/RCA observer”方案的形成过程。它是本次
设计工作的有界 worklog，不是运行时流程，也不会要求每个业务任务重复执行这些阶段。

## 目标

让本机 Hermes observer 在不修改原 RCA 的前提下，用飞书 Aily 企业知识解释术语和
补充业务背景。只有确定性注册术语命中或 stdin 显式查询才触发 business；无信号时
不调用 Aily 或 Web，原 RCA 继续。知识只帮助人工理解和聚焦后续分析，不能替代 issue、
MCAP、日志、代码或 live runtime 证据；任何检索失败都不改变已经完成或正在运行的原 RCA。

本轮按 owner 提供的方法完成设计，但这些步骤不进入运行时：

| 设计方法 | 本轮产物 |
| --- | --- |
| 盲区扫描 | 本文第 1 节的身份、grounding、幂等、本机边界和 ACL 风险。 |
| 先出原型 | 第 2 节的本机零副作用 observer。 |
| 反向采访 | 第 3 节的 owner 决定及风险接受。 |
| 给参照物 | 第 4 节的真实候选代码、测试报告和只读 RCA 契约。 |
| 实施计划 | 第 6 节按能力优先排列的本机实施顺序。 |
| Log 笔记 | 本文件，记录已确认、已否决和剩余缺口。 |
| 交接文档 | [集成交接](feishu-aily-business-integration-handoff.md)。 |
| 出题考你 | 交接中的证据 Quiz。 |

## 1. 盲区扫描

2026-08-21 对候选 Aily 工具、UAT transport、gateway session identity、RCA outbox、
completed report 和 VM 边界做了只读扫描，识别出以下问题：

1. 现有模型工具把固定员工 UAT 绑定到活跃飞书 session；独立 user smoke 已证明无 session
   的本机 transport 可用，但尚没有只对 RCA observer 开放的运行时入口。
2. Agent Chat 的 `Completed + content` 不能证明调用了知识检索，也没有稳定来源契约。
3. Aily create 没有调用方幂等键；create 成功但响应丢失可能造成重复 conversation。
4. 把最长 120 秒的查询放进 dispatcher、outbox 或 VM 会耦合 lease、重试和业务故障域。
5. 固定员工 UAT 会带来权限代理、审计主体不一致、权限漂移和 token 生命周期风险。
6. completed report 可能包含 ID、日志、用户信息或结论，不能整段发送给 Aily。
7. 如果为补查新增 VM gap、业务 schema 或 artifact，就违反“能力只在本机”的边界。
8. 共享 receipt 中低熵业务词不能只用裸 SHA，否则可被字典反推。

最新结论不是继续改业务契约，而是缩小承载面：首版只做本机 Hermes host 上的独立
RCA observer。固定员工 UAT 可被该 observer 复用；身份代理、审计和生命周期风险已经
owner 接受并延后专项治理，不是当前能力上线门。身份必须精确核验，凭据仍由隔离 broker
持有，且这项风险接受不授权 VM 下发、通用对话借权或扩大输出受众。

## 2. 最小原型

首版目标是本机、只读、可独立停用的 observer。当前已经有手动 shadow 原型文件
`~/.hermes/scripts/pnc_rca_aily_shadow_observer.py` 和入口 `~/bin/pnc-rca-aily-shadow`，
但它未启用、未注册、不是 daemon，也没有 consumer seam；它不能把结果回灌给正在执行的
RCA。`inspect` 和 `--provider dry-run` 不联网，真实 provider 当前禁用/不可用，且没有
耐久 create/poll 恢复，因此不能把原型写成已上线能力：

```text
local RCA locator / completed report (read-only)
  -> local Hermes RCA observer
  -> deterministic route + bounded query builder
  -> isolated broker + fixed Hu Zihao UAT + exact identity check
  -> owner-only local context / safe receipt
  -> post-completion deterministic registered-term extraction
  -> at most one round / two bounded supplemental queries
```

observer 不修改 dispatcher、outbox、execution request、VM goal、RCA schema、报告、
delivery manifest 或业务 artifact。首轮可读取已物化但仍在进行态的 sealed request；它可以
读取 completed report 仅用于第二阶段，
但所有新增状态都写入 owner-only 的 Hermes 本机状态区。停掉 observer 即完整回滚。

第一阶段只有确定性注册术语或显式 stdin 查询才触发；仅有 `task_type=rca` 不触发。
无信号规范状态为 `not_required`，手动原型显示别名 `not_triggered`，两者都不创建
provider job、不调用 Aily、不降级 Web。第二阶段不再等待 VM 产出 gap；未来 observer
只有在 sealed delivery manifest/contract 存在且身份/hash 校验通过后，才在原报告已经
完成时按版本化规则从允许字段提取、排序、去重和截断**新注册术语**，命中时至多执行
一轮、两条补查 query。缺失或不匹配时 fail closed，只记本机状态。报告文本始终按数据
处理，不能作为 prompt 指令原样执行。

代码复核一度考虑在 dispatcher 内 enqueue，也考虑让 VM 产出 gap artifact。最新本机
边界同时否决这两条路径：即使网络失败处理完善，它们仍会把个人凭据能力或新增契约带进
业务链。首版不需要等待正式 RCA 分支，也没有任何业务分支合入步骤。

## 3. Owner 决定

截至 2026-08-21，owner 的最新决定如下；新决定取代此前“首版必须先建服务身份”和
“二阶段必须由 VM 发 gap”的结论：

| 问题 | Owner 决定 | 方案影响 |
| --- | --- | --- |
| 无人值守 RCA 用什么身份？ | 首版复用胡子豪固定 UAT，能力优先。 | 仅本机 RCA observer 可用；每次调用精确核验 `open_id + union_id`，通过隔离 broker 获取 token。 |
| 身份代理、审计和生命周期风险是否阻断？ | 接受并延后专项治理。 | 登记为 `accepted/deferred`，不再要求专用服务身份或完整 ACL 治理作为首版门禁。 |
| 能力部署在哪里？ | 只在本机 Hermes host。 | 不进入 PNC/RCA 业务仓、VM、业务 schema、报告或 artifact，也不等待业务分支。 |
| 检索失败是否阻断 RCA？ | 不阻断。 | 所有失败状态只结束本地增强，原 RCA、报告和投递保持不变。 |
| 什么触发第一阶段？ | 确定性注册术语或显式 stdin 查询。 | 无信号（包括仅 `task_type=rca`）为 `not_required`/`not_triggered`，不调用 Aily 或 Web。 |
| 分析后出现的新术语如何处理？ | completed report 后本机确定性提取并补查。 | 不新增 VM gap；原 task 不等待、不 resume、不重投递。 |

风险接受的范围是“先获得本机能力”，不是把胡子豪 UAT 变成通用委托身份。其他飞书
用户、普通 Hermes 对话、CLI/API 调用方、业务 worker 和 VM 都不能借用该 UAT。

## 4. 参照物

本设计使用以下参照物，不将任何一份摘要当 live execution truth：

| 参照物 | 用途 |
| --- | --- |
| `tools/feishu_aily_agent_tool.py` | 当前候选的 Completed-only 状态机；不是首版本机 observer 已实现证明。 |
| `tools/feishu_aily_agent_user_transport.py` | 已通过 standalone canary 的 UAT transport、stdin、deadline、输出与进程隔离参照。 |
| `scripts/pnc_rca_outbox_dispatcher.py` | 只读核对原链边界；首版不得修改或调用内联网络查询。 |
| `gateway/pnc_rca_schema.py` | 只读核对现有业务契约；首版不得增加字段。 |
| `docs/feishu-aily-agent-test-report.md` | 候选测试和 session-derived UAT canary 边界。 |
| completed RCA report | 二阶段只读输入；不能被修改、复制进查询或扩展其 schema。 |

真实 canary 只能证明曾获得内部 OOI 相关文本，不能证明 no-public-web、逐答案来源或
grounding，也不能证明本机后台 observer 已实现。正式启用 observer 前必须形成新的
task-owned 安全 receipt，但不要求把内部答案或身份写入共享产物。

## 5. 已锁定设计

- 承载位置：仅本机 Hermes host 的 owner-only RCA observer、配置和状态。
- 运行身份：首版复用胡子豪固定 UAT；broker 每次调用精确核验配置中的
  `open_id + union_id`，不按姓名授权。
- 暴露面：UAT 不注册成通用对话工具，不提供给其他会话、CLI/API、resident 业务
  worker、outbox 或 VM；owner-only 人工 smoke 仅用于验收和诊断。
- 输入：只读本机 locator、允许字段和 completed report；query builder 严格排除完整
  issue/report、ID、URL、PDCL、帧、日志、用户/评论、附件和既有根因。
- 第一阶段：命中确定性注册术语或 stdin 显式查询才做 required attempt；仅 `task_type=rca`
  或无注册术语为 `not_required`/原型 `not_triggered`，不调用 Aily 或 Web。
- 第二阶段：报告完成后由本机确定性 extractor 产生有界的新注册术语集合；命中时至多
  补查一轮、两条 query，不依赖 VM gap、业务 schema 或新 artifact。
- Agent：目标策略是企业知识检索、无公网、无写工具和副作用 MCP；缺少逐回答来源时
  结果只能标为 `answer_only`。
- 影响：`observe_only` 或 `reference_only`，永远不是 execution evidence，不自动回写
  原报告或投递渠道。
- 故障：任何身份、网络、状态、解析、存储或检索失败都只结束本地增强。
- 持久化：仅 owner-only 本机状态；create/poll 必须有界，不确定 create 不盲目重建。
- 风险：权限代理、审计归属、ACL 漂移、token 到期/撤权登记为首版
  `accepted/deferred`，后续可迁移到专用服务身份，但不是当前门禁。

以下方向已被 owner 决定明确取代：

- 首版必须等待专用 RCA service identity；
- 等待正式 RCA 业务分支后再实现或移植；
- 在 PNC/RCA 业务仓中新增 provider、router、schema 或 artifact；
- 由 VM 写 `business_knowledge_gap_v1`，再由 host 写 addendum；
- `business=required` 失败后阻断 VM 或暂缓投递；
- 把固定员工 UAT 暴露为通用对话借权能力。

## 6. 能力优先实施顺序

1. 冻结本机边界 oracle：确认业务仓、dispatcher、VM、业务 schema/产物在启用前后无
   改动，并确定 owner-only 本地状态目录。
2. 从现有候选中复用最小 transport 逻辑，形成 observer 专用隔离 broker；每次请求先
   精确核验胡子豪 `open_id + union_id`，错误身份在 create 前失败。
3. 保持手动 shadow 原型的 `inspect`/`dry-run` 无网络、未注册、非 daemon 行为；在
   确定性触发信号命中时再实现本机只读 observer。不得注册为通用模型工具，也不获得
   业务 outbox lease。
4. 实现 completed report 的确定性新注册术语 extractor：版本化允许字段、排序、去重、上限和
   query allowlist；所有报告内容按不可信数据处理。
5. 在真实 provider 仍禁用期间只验证 dry-run；先拆分并耐久封存 create/poll，再评审
   本机 consumer seam。对新注册术语的补查最多一轮、两条 query，结果只落 owner-only
   本机 reference/receipt；任何失败不重试原 RCA、不修改报告、不触发投递。
6. 运行 focused 单测、身份负例、本机路径/写入审计、故障矩阵；真实 OOI canary 只能
   在 provider 明确启用且 observer 有 consumer seam 后执行，当前不得宣称已帮助 RCA。
7. 专用服务身份、细粒度 ACL、审计映射和 token 轮换自动化进入后续专项，不阻断首版。

该顺序没有正式业务分支、cherry-pick、VM 发布、业务 schema migration、gateway
production materialize 或业务仓合入步骤。

## 7. 当前验证与缺口

已验证：

- 候选 Aily user transport、tool、smoke、session identity 和 CLI 配置测试曾通过；详情见
  测试报告。
- 固定员工 UAT 的真实 API canary 曾对内部 OOI 问题返回业务内容；这只是 standalone
  transport 能力，不是 observer consumer 或 RCA 回灌证据。
- 手动 shadow 原型文件和 wrapper 已存在，但当前仅可作为离线 planner 参照：未启用、未
  注册、非 daemon；`inspect`/`dry-run` 无网络，真实 provider 禁用/不可用且无耐久恢复。
- 2026-08-21 本机独立进程在没有飞书 session context 时通过 user smoke 完成固定 UAT
  调用：`ok=true`、`status=Completed`、`user_identity_verified=true`、`poll_count=17`、
  `answer_available=true`、`answer_length=2300`、`text_item_count=3`、
  `artifact_count=0`；默认输出没有答案正文、token 或 secret。这证明 standalone 后台
  transport 能力，不证明 RCA observer 已接入、grounding/source 或 no-public-web。
- 上述成功 canary 之前有一次 PTY EOF 操作在已经进入 poll 后被人工中断，没有终态，
  不计为成功或失败回执；成功证据来自随后使用非交互 stdin 的独立调用。
- 原 dispatcher、VM fixed goal、报告和投递边界已只读核对，支持将增强完全移出业务链。
- Owner 已明确批准本机 observer 复用胡子豪 UAT，并接受/延后相关身份风险。

仍未实现或未验证：

- 本机后台 RCA observer、确定性 trigger 和只对 observer 开放的 broker 调用路径；standalone
  smoke 已证明 transport 能核验身份，但未证明运行时调用面隔离。
- consumer seam 未放开；当前任何 shadow 结果都不能回灌正在执行的 RCA。
- completed report 的确定性术语 extractor 和第二阶段补查；
- owner-only 本地 context/receipt、create-unknown 恢复和故障矩阵；
- 本机 observer 的真实 OOI canary、无通用对话暴露检查和写入路径审计；
- Aily Agent 无公网/无写工具的可审 fingerprint 或明确的 `answer_only` 降级证据。

这些缺口意味着自动 trigger、真实 provider、consumer seam 和 RCA 无感增强尚未实现、
尚未启用，也未发布到 production gateway；仅手动 offline shadow planner 已存在。它们
不再包含“等待正式 RCA 分支”或“先完成专用服务身份”。本轮候选 worktree 只更新设计、
交接和 Python 合同断言，不修改候选 transport/tool/runtime 实现、配置、凭据、resident
或生产；本机 shadow planner 单独位于 owner-only Hermes 路径。

下一恢复入口是
[集成交接](feishu-aily-business-integration-handoff.md)，不是本聊天记录。

## 8. 证据 Quiz 结果

2026-08-21 按 owner 最新决定重写交接 Quiz。文档自检不代表实现验收；实现完成后必须
用代码、测试或本机 receipt 逐题重新取证。

| 题目 | 当前设计答案 | 所需实现证据 |
| --- | --- | --- |
| 1. Aily 超时是否阻断 RCA？ | 否。 | observer 与原 dispatcher/VM/report/delivery 无写路径。 |
| 2. Aily 文本是否是根因证据？ | 否。 | 输出仅在 owner-only 本地 reference 状态。 |
| 3. 首版后台 RCA 用谁的身份？ | 胡子豪固定 UAT。 | isolated broker 每次精确核验 `open_id + union_id`。 |
| 4. 谁能借用该 UAT？ | 仅本机 RCA observer。 | 无通用对话、其他用户、CLI/API、outbox 或 VM 调用面。 |
| 5. 已接受哪些风险？ | 权限代理、审计归属、ACL 漂移和生命周期。 | 风险登记为 `accepted/deferred`，不伪装成已消除。 |
| 6. 新术语如何补查？ | 本机只读 completed report 后确定性提取。 | 仅新注册术语触发，最多一轮两条 query；无 VM gap、schema、artifact 或 resume。 |
| 7. 哪些位置不得改变？ | 业务仓、VM、业务 schema/产物及原报告/投递。 | 路径和 diff 审计为零写入。 |
| 8. 无注册术语时做什么？ | `not_required`/原型 `not_triggered`。 | 不调用 Aily 或 Web，原 RCA 继续。 |
| 9. 是否等待正式 RCA 分支？ | 否。 | 本机实现与业务分支、合入和发布解耦。 |
| 10. 什么允许本机启用？ | broker/身份、隔离、故障、写入边界、consumer seam 和真实能力证据通过。 | `Completed + content` 或本文自检不能单独作为证据。 |

当前只完成设计口径更新，不得把 Quiz 答案解释为 observer 已实现、已启用或已发布。
