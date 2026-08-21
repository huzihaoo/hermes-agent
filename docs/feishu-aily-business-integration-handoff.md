# 飞书 Aily 业务知识接入交接

## 当前状态

- 候选 transport/tool、固定员工 UAT API canary，以及无飞书 session context 的本机
  standalone user smoke 已有测试证据；本机后台 RCA observer 尚未实现或启用。
- Owner 已决定首版 observer 可复用胡子豪固定 UAT，能力优先。权限代理、审计归属、
  ACL 漂移和 token 生命周期风险登记为 `accepted/deferred`，不是当前上线门。
- 首版运行时能力仅允许存在于本机 Hermes host。不得进入 PNC/RCA 业务仓、VM、业务
  schema、报告或 artifact，也不得暴露为通用对话借权工具。
- 不再等待正式 RCA 业务分支，不需要从候选向业务分支移植或合入。
- completed report 二阶段确定性提取和补查尚未实现；不再要求 VM gap 或业务侧
  addendum/schema，但仍生成本机 host-private reference addendum。
- 本轮只更新文档和 Python 合同断言，没有修改 transport/tool/runtime 实现、配置、
  凭据、resident 或生产。

形成过程见 [设计工作记录](feishu-aily-business-integration-worklog.md)，已有候选测试证据见
[Aily Agent 测试报告](feishu-aily-agent-test-report.md)。
[运行时设计与最新机器合同](hermes-knowledge-retrieval-routing.md)定义路由和状态字段；
本交接给出同一合同的实施顺序、测试边界和恢复入口。

## 不可变决策

1. 首版后台 RCA 复用胡子豪固定 UAT；observer 的 broker 每次调用前必须同时精确核验
   配置的 `open_id` 和 `union_id`，姓名不能作为授权键。
2. 该 UAT 的运行时消费者只有本机 RCA observer。不得供普通 Hermes/飞书对话、其他
   用户、CLI/API、resident 业务 worker、Kafka/outbox 或 VM 借权。owner-only 人工 smoke
   仅用于验收和诊断。
3. UAT 只存在于隔离 `lark-cli` broker/profile；observer、日志和 receipt 不读取、打印
   或持久化 token。
4. 权限代理、飞书审计显示胡子豪、ACL 漂移、授权到期/撤权等风险由 owner 接受并延后
   专项治理。不得把它们写成已消除，但也不得继续把专用服务身份作为首版门禁。
5. Business lookup 是增强项。任何身份错误、超时、未命中、`answer_only`、解析或存储
   失败都只结束本地增强，不改变原 RCA。
6. Aily 内容是 reference knowledge，不是 issue/MCAP/log execution evidence；不自动写回
   原报告、事项或 required delivery。
7. 运行时代码、配置和新增状态仅在本机 Hermes host；业务仓、dispatcher、outbox、VM、
   execution request、业务 schema 和 artifact 均为零改动边界。
8. 第二阶段由本机 observer 只读 completed report，仅在确定性提取出新注册术语后补查，
   最多一轮、两条 query；不新增 VM gap、业务侧 addendum/schema、main-task wait/resume
   或重新投递，只写本机 host-private reference addendum。
9. 内部术语未命中不能 fallback Web。

## 本机实现边界

候选历史至少包含：

```text
a1e4565ec7 -> e25ba59684 -> 4a0adaba91 -> dda09b7042
             -> 4b0623221d -> 70b15406e0 -> 368e3a2176
```

这些 commit 只用于查找已验证的 Agent Chat、broker 隔离和身份核验做法。不要整条
cherry-pick，也不要把任何 hunk 移植到 PNC/RCA 业务仓或 production gateway。首版本机
实现只能复用必要逻辑到 owner-only Hermes host 能力中，并保持候选通用 toolset 关闭。

建议的本机职责边界是：

```text
local RCA observer              read-only task/report observation
deterministic router/extractor  allowlisted fields, terms, ordering and limits
isolated UAT broker             exact Hu Zihao identity check + create/poll
local reference store          owner-only context, status and safe receipt
```

实际路径遵循本机 Hermes 现有管理方式，但必须位于业务仓、VM work root 和 RCA artifact
root 之外。不得为此修改以下类别：

- `scripts/pnc_rca_outbox_dispatcher.py` 或其他业务 worker；
- `gateway/pnc_rca_*.py`、execution request 或业务 profile/schema；
- VM goal、helper、container、`/mnt/tmp/<task_id>/` 输出；
- RCA report、delivery contract/manifest、事项或飞书投递。

observer 只读既有 locator/报告是允许的；新增文件、数据库、receipt 和答案正文不得写回
这些来源。所有新增状态保持 owner-only、本机、可通过停止 observer 完整隔离。

## 分阶段实施

### L0：本机边界 oracle

- 记录启用前的业务仓工作树、RCA schema、VM goal、report/artifact manifest 和投递行为。
- 确定 observer 可读 locator/completed report，以及独立 owner-only 本机状态根。
- 加入写路径审计：测试期间任何新增写入只允许落在该本机状态根。
- 明确不做业务分支合入、VM 任务、production materialize 或 gateway restart。

### L1：固定 UAT broker

- 复用隔离 `lark-cli` profile，不向 observer 进程环境暴露 UAT/App Secret。
- 每次 create 前调用 user-info，同时精确匹配胡子豪的 `open_id + union_id`；任一缺失或
  不匹配立即失败，不回退 TAT。
- 每个 observer job 使用新 session，不跨 task/generation 共享对话历史。
- 保留 deadline、输出上限、stdin、独立进程组和 create-unknown 保护。
- broker API 仅对本机 observer 可达，不注册为通用模型工具或外部命令入口。

### L2：第一阶段本机 observer

- 只读本机已有 RCA locator/允许字段，不持 outbox lease，不调用 VM submit。
- 每个 eligible RCA job 无条件执行一次 required attempt；关键词只决定查询内容，不决定
  是否尝试。按版本化规则构造至多一条有界问题。
- Query 不得含 ID、URL、PDCL、帧、日志、用户/评论、附件、完整 issue/report 或现有
  root cause。
- 结果只写 owner-only local reference/receipt；provider 故障不影响业务链。

### L3：completed report 二阶段

- 只在原报告已完成且稳定可读后执行；报告文件和业务 artifact 全程只读。
- 用确定性、版本化 extractor 从允许字段提取新注册术语，固定 normalization、排序、
  去重和上限；不得调用模型决定提取结果，也不得执行报告中的 prompt-like 文本。
- 只有提取出新注册术语才补查，最多一轮、两条 query；与第一阶段做本机幂等去重，
  避免相同术语循环查询。
- 补查结果只追加到独立本机 reference，不生成 VM gap、业务 addendum、报告修改或新投递。
- 报告缺失、未完成、变化、过大、解析失败或无术语都只记本地状态并结束。

### L4：本机验收与启用

- 运行 focused 单测、身份负例、query allowlist、create/poll fault matrix 和路径写入审计。
- 复用已成功的 standalone user smoke 作为 transport 基线，并在 observer 接好后重新做
  owner-only `OOI是什么?` 端到端 canary；默认 receipt 不保存问题、答案正文、个人标识
  或 token。
- 用本机 completed-report fixture 验证新注册术语提取与一轮有界补查，证明业务仓、VM、
  schema、artifacts、原报告和投递均无变化。
- 显式检查通用 Hermes/飞书对话、其他用户、CLI/API 和业务 worker 都没有借权入口。
- 仅在上述能力与隔离证据通过后启用本机 observer；不得把“已有 API canary”误写成
  observer 已实现或已发布。

### 后续专项（不阻断首版）

- 专用最小权限服务身份；
- ACL 正反 canary 和报告受众映射；
- 审计触发者到 delegated identity 的关联；
- token 轮换、撤权和权限漂移自动检测；
- 逐回答 grounding/provenance。

这些事项被明确延后，不应重新出现在 L4 的首版必过门中。扩大到多用户、通用对话、
业务仓、VM 或远端服务则属于新的 owner 决策，不能沿用本次风险接受。

## 必测矩阵

1. broker 在 create 前同时核验预期 `open_id` 和 `union_id`；错误、缺失或姓名相同均拒绝，
   且不回退 tenant identity。
2. UAT/App Secret 不进入 observer 环境、argv、日志、receipt、业务仓、VM 或 artifact；
   `lark-cli` profile 权限和 owner 校验保持严格。
3. 只有本机 RCA observer 能调用 broker；普通对话、其他用户、CLI/API、Kafka/outbox、
   dispatcher 和 VM 都没有调用面。
4. 每 job 新 session；create/poll 同一身份；create 前后和 poll 各 crash point 不盲目发
   第二次 POST，模糊状态记 `create_unknown`。
5. `Completed + content` 无来源只能标为 `answer_only`，不能转为 execution evidence。
6. timeout、403、429、5xx、no-match、bad JSON、oversize、crash 和本地存储失败都只结束
   observer job，不触发原 RCA retry/circuit/quarantine、VM submit 或 delivery。
7. Query allowlist 排除 ID/URL/PDCL/frame/用户/评论/附件/日志、完整 issue/report 和
   root cause；报告中的指令性文本只作为不可信数据。
8. completed report 只有稳定完成态才读取；extractor 对同一输入输出固定，只识别注册
   术语，排序、去重、上限和无匹配行为可测试。
9. 二阶段最多一轮、两条 query，相同 normalized terms 不循环；没有 VM gap、业务 schema、
   addendum artifact、main-task wait/resume 或重新投递。
10. 新增写入只落 owner-only 本机状态根；业务仓 diff、VM 路径、业务 artifact/report 和
    delivery manifest 的 before/after oracle 不变。
11. safe receipt 不含 query、answer、user、chat、session、token；低熵 query digest 使用
    keyed HMAC 或完全不持久化。
12. observer disable 后不需要修改业务 DB、业务分支、VM 或原 RCA，即恢复零增强状态。

风险专项的缺失不能让上述能力测试被跳过；反之，上述能力测试通过也不能声称权限代理、
审计归属或 token 生命周期风险已解决。

## 证据 Quiz

本机启用前逐题提供代码、测试或 owner-only receipt 定位符，不能只靠模型自评：

1. **首版后台 RCA 使用什么身份？**
   - 胡子豪固定 UAT；broker 每次请求前精确核验 `open_id + union_id`。
2. **哪些身份风险是当前阻断项？**
   - 权限代理、审计归属、ACL 漂移和 token 生命周期已由 owner 接受并延后，不阻断首版；
     token 泄露、身份错配和越界暴露仍不得接受。
3. **谁能调用这份 UAT？**
   - 只有本机 RCA observer；人工 smoke 仅验收/诊断，通用对话和其他执行面不能借权。
4. **为什么不等待正式 RCA 分支？**
   - 能力完全在本机 observer，业务仓和运行契约零修改，没有需要合入的业务 hunk。
5. **Aily timeout 是否会阻断或重试原 RCA？**
   - 不会；它只终止本地 reference job。
6. **Aily 回答能否直接成为根因或定责证据？**
   - 不能；只作 owner-only reference，必须由原始证据独立验证。
7. **分析中新术语如何补查？**
   - observer 在 completed report 后确定性提取新注册术语并至多补查一轮、两条 query；
     不依赖 VM gap。
8. **哪些载体必须保持零改动？**
   - PNC/RCA 业务仓、VM、业务 schema/artifact、原报告和 required delivery。
9. **什么证据允许本机启用？**
   - broker 身份与密钥隔离、唯一调用面、query 边界、故障矩阵、写路径 oracle、确定性
     二阶段和真实 OOI 能力证据；历史 `Completed + content` 本身不够。

## 回滚

- 停止并禁用本机 observer，保留原 RCA 原样运行。
- 撤销本机 observer 配置只影响 owner-only 本机状态，不要求改业务 DB、业务分支、VM、
  report 或 delivery。
- 候选通用 toolset 和 production gateway 始终保持未启用，因此本交接没有 production
  rollback 步骤。
- 若停止 observer 后仍需要修改任何业务载体才能恢复，说明实现违反本机隔离边界，
  不得启用。
