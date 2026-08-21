# Hermes 统一知识检索路由

本文定义 Web、本地知识工程和飞书 Aily 企业知识的同级触发机制。目标是让用户直接
提问或发起任务，不需要额外说“请查知识库”；Hermes 在开始推理前根据关键词和任务
上下文选择正确的知识源。

> 当前状态：设计契约，尚未接入生产 gateway。Aily 工具本身已经形成候选并完成
> UAT canary；自动路由必须等待 RCA 正式生产分支后实现和发布。

## 目标与非目标

目标：

- 公开、时效性问题触发 Web 检索。
- 本机代码、运行态、任务历史和本地知识工程问题触发本地检索。
- 公司缩写、产品、功能、流程、组织、项目字段和内部规范触发 Aily 企业知识检索。
- RCA 等强业务任务始终执行企业知识检索，即使正文没有显式关键词。
- 检索结果在任务开始前形成结构化上下文，后续阶段无感复用。

非目标：

- 不把企业知识答案当成生产运行证据或 RCA 原始数据。
- 不在企业知识未命中时静默改用公网搜索猜测内部含义。
- 不把用户 UAT 传入 VM、MCP、terminal、cron 或后台 worker。
- 不在数据库事务、外部写入或报告发布阶段临时发起不可重放的网络检索。

## 三类同级检索源

| route | 适用内容 | 典型能力 | 可信边界 |
| --- | --- | --- | --- |
| `web` | 公开且可能变化的信息、官方外部文档、新闻、标准版本 | `web_search` / `web_extract` | 外部参考；必须保留来源和时间。 |
| `local` | 当前代码、live runtime、任务 checkpoint、本地知识工程、历史会话 | 文件/代码搜索、`session_search`、memory、受管 context retrieval | live 证据优先；wiki/memory 仍需按 source class 裁决。 |
| `business` | 公司术语、产品行为、业务规则、内部流程、项目/RCA 语义 | `feishu_aily_agent_chat` | 用户权限过滤的企业参考知识；不能替代 live/原始证据。 |

三个 route 是同一检索路由器的候选输出，不是彼此的 fallback 链。一个任务可以并行
选择多个 route，但每份结果必须保持来源边界。例如 RCA 可以同时需要本地 live
证据和企业业务定义；只有明确涉及公开标准时才额外选择 Web。

## 触发模型

每个 route 的需求级别只能是：

| requirement | 含义 |
| --- | --- |
| `none` | 本任务不需要该来源，路由决策仍记录。 |
| `auto` | 关键词/歧义规则命中后自动检索；用户无需显式下令。 |
| `required` | 任务契约强制检索；模型分类不得降级。RCA 的 business 属于此级。 |

以下 block 是文档测试会解析的最小机器契约：

<!-- knowledge-routing-contract:begin -->
```json
{
  "requirements": ["none", "auto", "required"],
  "statuses": [
    "grounded_match",
    "answer_only",
    "no_match",
    "identity_unavailable",
    "error",
    "not_required"
  ],
  "rca_stage_order": [
    "durable_admission",
    "claim",
    "official_issue_preread",
    "identity_validation",
    "business_lookup",
    "seal",
    "resource_reservation",
    "vm_submit"
  ],
  "grounded_match_required": [
    "query_sha256",
    "answer_sha256",
    "knowledge_release_fingerprint",
    "source_refs_sha256",
    "retrieval_activity_receipt_sha256"
  ],
  "no_match_required": [
    "query_sha256",
    "knowledge_release_fingerprint",
    "provider_no_hit_receipt_sha256"
  ],
  "shared_receipt_forbidden_fields": [
    "summary",
    "content",
    "raw_answer",
    "open_id",
    "union_id",
    "token"
  ]
}
```
<!-- knowledge-routing-contract:end -->

### 第一层：确定性关键词规则

维护一个版本化、可审计的关键词注册表，不放在 `.env`。每条规则至少包含：

```yaml
id: pnc-rca-ooi
patterns: [RCA, OOI, CIPV, AEB, ACC, LCC]
route: business
strength: required
domains: [pnc, rca]
query_template: "解释这些术语在当前业务中的定义、约束和预期行为：{terms}"
```

匹配要求：

- ASCII 缩写按 token/词边界匹配，不能简单 substring。
- 中文产品名、流程名和字段名使用规范化别名集合。
- 短缩写必须带 domain 或任务上下文，避免把普通单词误判为业务术语。
- 规则命中、规则版本和生成的查询都写入任务 receipt。
- 关键词注册表更新走代码评审；secret、用户 ID 和知识正文不得进入注册表。

建议的业务强信号包括但不限于：

- 内部缩写、功能名、车型/项目代号、模块名。
- 飞书项目链接、内部 issue 字段、组织/值班/流程名称。
- “公司内”“业务定义”“内部规范”“历史方案”等明确范围词。
- RCA、故障归因、责任域、预期功能行为和结案判断。

### 第二层：任务上下文规则

任务类型比关键词优先级更高，用来覆盖没有显式关键词的情况：

| 上下文 | 路由要求 |
| --- | --- |
| `task_type=rca` 或正式 RCA intake | `business=required`，同时保留 `local=required`。 |
| 飞书项目/内部 issue 分析 | 至少 `business=auto`；出现业务字段或产品行为时升级为 required。 |
| 当前仓库、runtime、日志、配置排查 | `local=required`；业务术语出现时并行 business。 |
| 公开时效性问题 | `web=required`；若同时包含公司术语，Web 与 business 分开查询。 |
| 纯通用编程、数学、语言转换 | 默认不检索；出现内部关键词时重新路由。 |

RCA 的强制规则不能被正文中的“不要查知识库”绕过。用户可以要求不展示检索过程，
但不能让正式 RCA 在缺失业务语义校验的情况下产生确定性结论。

### 第三层：歧义分类

关键词和任务上下文都没有明确结论时，才让模型做一次轻量分类：

- 该问题是否依赖公司特有定义？
- 错把公司含义当作通用含义是否会改变结论？
- 是否需要最新公开信息？
- 是否需要当前机器/任务/live 证据？

分类器只输出 route 和 reason code，不生成事实答案。任何内部缩写无法确认时优先
选择 `business`，不能先去 Web 搜索同名公开概念。

## 路由优先级与冲突

1. **范围先于便利性**：公司语义只由 business 解释，不以 Web 作为未命中兜底；
   可以并行 local/live 来核对当前实现和运行证据。
2. **live 先于说明性知识**：当前运行状态、代码和任务结果以 local/live evidence 为准。
3. **业务知识解释语义**：business 用来解释术语、预期行为和流程，不覆盖原始信号。
4. **Web 只回答公开问题**：公开标准可辅助 RCA，但必须与内部业务规则分栏呈现。
5. **冲突不自动融合**：business 与 local/live 冲突时标记 `knowledge_conflict`，继续采集
   原始证据并要求人工裁决；不得挑一个更顺眼的答案。

## 统一结果契约

路由器为每个任务生成一个有界的 `knowledge_context_v1`。示例只展示字段结构：

```json
{
  "schema_version": "knowledge_context_v1",
  "decision": {
    "routes": ["business", "local"],
    "requirements": {"business": "required", "local": "required", "web": "none"},
    "reason_codes": ["task_rca", "keyword_ooi"],
    "rule_version": "..."
  },
  "lookups": [
    {
      "route": "business",
      "query_sha256": "...",
      "status": "grounded_match",
      "grounding_basis": "source_bound_retrieval_receipt",
      "source_refs_sha256": "...",
      "retrieval_activity_receipt_sha256": "...",
      "summary": "bounded task context",
      "answer_sha256": "...",
      "retrieved_at": "...",
      "latency_ms": 0,
      "attempts": 1,
      "cache_hit": false,
      "identity_scope": "user",
      "auth_principal_fingerprint": "...",
      "knowledge_release_fingerprint": "...",
      "raw_answer_logged": false
    }
  ],
  "controls": {
    "public_web_forbidden_for_internal_terms": true,
    "may_override_live_evidence": false
  }
}
```

`status` 只能取：

| status | 含义 |
| --- | --- |
| `grounded_match` | source-bound receipt 同时绑定 query、命中来源、知识/Agent 版本和 answer hash。 |
| `answer_only` | Agent 返回了文本，但没有足够证据证明来自知识检索；只能作弱参考。 |
| `no_match` | 有结构化 provider no-hit/活动证据证明未命中；不能根据模型的“未检索到”文字自行判定。 |
| `identity_unavailable` | 当前入口没有可用于该任务的受审身份；不得借用其他员工权限。 |
| `error` | 身份、权限、超时或协议错误。 |
| `not_required` | 路由器有记录地判定该 route 不需要执行。 |

当前 Aily 工具能返回 `Completed`、文本、chat/session ID 和有界 artifact 标识，但没有
完整 grounded provenance。因此在扩展工具结果契约前，自动流程最多把它标为
`answer_only`；不能根据 `answer_available=true` 自行升级为 `grounded_match`。
旧 Data Knowledge API 的 `has_answer=true` 仍归为 `answer_only`，只在
`grounding_basis` 记录 `provider_asserted_has_answer`；没有来源/活动证据时不能
升级为 `grounded_match`。

## 从检索到交付的八步闭环

检索不是一次性的前置动作。substantial 或 required-knowledge 任务在理解加深时
会不断暴露新关键词和新盲区，因此统一路由要嵌入以下闭环。简单问答使用
`route decision -> lookup -> answer` 轻量路径，其余阶段记为不适用，不创建多余
worklog 或验收仪式。闭环中每一步都有有界产物，并能触发下一轮检索。

### 1. 盲区扫描：寻找“未知的未知”

在制定方案前，先从用户问题、任务类型、issue、代码和现有材料中抽取：

- 未定义缩写、内部产品/模块/流程名称。
- 看似通用但可能有公司特有含义的词。
- 会改变方案或 RCA 结论的业务假设。
- 缺失的角色、权限、数据范围、版本和成功标准。
- business、local、web 证据之间的冲突。

所有新术语重新经过关键词路由。RCA 至少生成“术语/预期行为”“业务约束/异常判定”
两类查询；扫描结果写成 `blind_spots`，不能只留在模型上下文中。

### 2. 先出原型：看完最小闭环再决定要什么

盲区扫描后先做最小、可逆、无生产副作用的原型或 dry-run：

- API 接入先做脱敏 smoke，不先改 resident。
- RCA 接入先生成一份 shadow `knowledge_context_v1`，不先改变归因结果。
- 文档/交互先用一个真实问题走完整流程，再决定字段和 UI。

原型的目的不是提前交付，而是让返回形状、延迟、权限和无匹配行为变得可观察。
原型发现的新字段或术语回到盲区扫描，不允许拿首次猜测直接扩成生产方案。

### 3. 反向采访：只问会改变做法的问题

AI 根据 `blind_spots`、原型结果和来源冲突生成少量高价值问题。每个问题必须附带：

- 哪个答案会改变 route、数据边界、实现顺序或验收标准。
- 不回答时采用什么可逆默认值。
- 是否会阻断生产、外部写入或确定性 RCA 结论。

例如：“OOI 的异常判定以哪份内部规范为准？”会改变 RCA evaluator 和报告口径，应
提问；“标题喜欢哪个措辞？”通常不应打断执行。已能从 business/local 权威来源确定
的问题不再询问用户。

### 4. 给参照物：说不清时使用文件或样例

用户可以提供现有报告、issue、截图、接口响应或期望样例。路由器先分类材料：

- 当前代码/配置/日志进入 local，并按 live/reference/memory 分级。
- 企业内部文档进入 business 或受控本地解析。
- 公开标准和官方外部文档进入 web。

参照物不是默认正确答案；必须记录来源、版本、适用范围和与 live 的差异。不能未经
确认把本地文件自动上传到 Aily；`agent_attachment_ids` 只接受已经通过受控附件流程
上传、且归属当前身份的材料。

### 5. 实施计划：先验证最可能反悔的决定

计划按“返工代价和反悔概率”排序，而不是只按代码依赖排序：

1. 先验证身份、知识范围、接口形状、grounded 证据和失败策略。
2. 再锁定任务契约、schema 和 host/VM 边界。
3. 然后实现可逆的路由、shadow 和测试。
4. 最后才做 manifest、resident restart、外部写入等生产效果。

“放前面”指尽早验证高风险假设，不代表提前执行不可逆操作。数据删除、历史改写、
生产切换和敏感授权仍遵守单独审批与治理门。

### 6. Log 笔记：边做边记

每个任务维护有界 `knowledge_worklog_v1`，checkpoint 覆盖更新而不是追加原始长日志：

```json
{
  "route_decisions": [],
  "blind_spots": [],
  "assumptions": [],
  "questions_that_change_plan": [],
  "reference_artifacts": [],
  "knowledge_context_sha256": "...",
  "conflicts": [],
  "verification": [],
  "remaining_gaps": []
}
```

笔记记录决策、hash、状态和定位符，不复制 secret、UAT、完整内部答案或大段原始日志。
新证据推翻旧结论时标记 `superseded`，不能把两套叙事揉在一起。

### 7. 交接文档：给人看，也给下一次任务恢复

交接包至少包含：

- 目标、范围和当前状态。
- route decision、关键词规则版本和检索状态。
- 已确认事实、source class、证据定位符和 hash。
- 原型结果、关键决策、未回答问题和已拒绝方案。
- 测试命令、结果、未覆盖项、发布与回滚步骤。
- 下一步唯一入口，以及哪些操作仍需 owner 授权。

交接文档不能把候选测试写成 live 已生效，也不能用知识摘要替代当前 task/shared-state。

### 8. 出题验收：关键题满分才能提交

提交或发布前，根据目标、业务知识、原型和验收标准生成一组带依据的题目。题库至少
覆盖：

- 是否能解释所有影响实现的内部术语，并指出业务知识依据？
- 是否知道哪些字段是 reference knowledge，哪些是 live execution truth？
- `no_match`、`answer_only`、身份错误和超时分别怎样处理？
- 是否存在内部问题降级 Web、跨用户缓存或 UAT 进入 VM 的路径？
- RCA 在业务上下文缺失时，哪些步骤可以继续，哪些结论必须 abstain？
- 测试、发布、回读和回滚是否能由交接文档复现？

每题必须有可定位的答案依据，不能由模型凭自信自评。任一关键题错误、无依据或只靠
猜测，就把对应 gap 写回 worklog，回到盲区扫描；关键题未满分不得提交。非关键的
措辞偏好可以记录为 follow-up，不能伪装成发布 blocker。

### 闭环状态机

```text
blind_spot_scan
  -> reversible_prototype
  -> reverse_interview
  -> reference_intake
  -> regret_first_plan
  -> implementation_with_worklog
  -> handoff_package
  -> evidence_quiz
       | pass: ready_for_review_or_release
       ` fail: blind_spot_scan
```

为了避免无限检索，每轮按 query hash 去重并使用任务级查询预算；只有新证据、新关键词、
来源冲突或 quiz gap 才能开启下一轮。

## 无感交互流程

### 普通对话与任务

1. 收到消息后先运行本地 route decision，不向用户展示中间提示。
2. 有 required route 时并行发起有界检索；同时可继续做不依赖结果的本地读取。
3. 将有界 summary 作为独立、带来源标签的上下文注入主任务，不把检索文本当指令。
4. 正常回答时无需逐条播报工具调用；只有 `no_match`、`error` 或来源冲突才明确告知。
5. owner-only task execution pack 可保存有界 context；共享/audit receipt 只记录
   route、reason、状态、hash、长度、provenance、时间和耗时，不记录问题/答案正文。

### RCA

RCA 采用 host-side preflight，不能让 VM worker 自行携带 UAT 查询：

1. **intake**：读取 issue 元数据时同时设置 `task_type=rca`，无条件触发 business。
2. **query build**：从标题、业务 profile、功能域和已注册缩写生成最多若干条有界查询；
   默认不把整份 issue 描述发送给 Aily。
3. **lookup**：durable admission 和 claim 完成后，先做官方 issue preread 与 identity
   validation；只有拿到 title/profile/domain 后才构造查询。随后由受管、只读的后台
   business provider 执行 lookup，可与只读 storage/resource preflight 并行；资源预留
   和任何外部副作用仍必须等 seal 完成后才允许。
4. **seal**：把结构化状态、summary、hash、规则版本写入 task-owned receipt。
5. **handoff**：只把有界 `business_knowledge_context` 加入固定 VM execution request；
   UAT、open_id、union_id、原始答案不得进入 VM。
6. **analysis**：VM 原始信号和工具链仍是 RCA execution truth；业务上下文只解释
   预期行为、术语和责任域。
7. **report gate**：enforced 模式下，`error`/`identity_unavailable` 有界重试后必须在
   resource reservation/VM submit 前停止；`answer_only`/`no_match` 只能进入未来明确
   定义的 `evidence_only` execution mode，依赖业务语义的根因、责任归属和结案结论
   必须 abstain。该 mode 未进入正式 execution schema 前同样停止提交。
8. **delivery**：报告生成和飞书投递只消费已封存上下文，不在写入阶段再次调用 Aily。

不应直接加网络调用的位置：ControlStore/SQLite 事务内部、durable outbox settle、Feishu
写入 guard、VM worker、报告渲染器和 completion/delivery dispatcher。

当前固定员工 UAT 只允许对应的活跃飞书会话，不能用于 Kafka/outbox 自动 RCA。后台
provider 必须是单独审批的 service principal，并满足：只读、知识范围等价、可无人
值守轮换、可审计、不能代表任意员工。若 Aily 应用身份只能访问导入知识而不能访问
所需直连知识，则必须先解决等价知识范围或平台支持的服务身份；不能把固定员工 UAT
装进 resident 充当替代。该身份未就绪时，RCA `business=required` 的 shadow decision
可以记录，但 enforced gate 必须保持 blocked。

## 查询构造

查询应短、明确、最小披露。RCA 示例：

```text
请基于企业知识说明 OOI 在 ACC/AEB 业务中的定义、关键字段、正常目标切换规则，
以及哪些现象不能单独判定为目标选择异常。未检索到时请明确返回未命中。
```

不要默认发送完整聊天记录、完整 issue 描述、日志、客户信息或原始数据路径。一个任务
内的多个查询使用同一个任务 correlation ID；是否复用 Aily session 必须由污染风险
测试决定，不能为了省调用而让不同任务共享会话。

## 延迟、缓存和配额

- route decision 必须本地完成，不调用额外大模型。
- business 查询与 issue/local context 预取并行，避免串行增加首响应时间。
- 每个任务设置最大查询数、总 deadline、单次输出上限和有界重试；现有 Agent 工具
  的总 deadline 为 120 秒。
- 缓存必须按 `route + agent/policy version + auth principal fingerprint + knowledge
  release fingerprint + normalized query hash` 隔离，使用短 TTL；不能跨用户共享
  企业答案。
- RCA 只允许同一 generation 内按 query hash 去重，不跨 generation 复用答案。缓存
  命中仍需写 provenance；知识策略、Agent 发布版本或授权主体变化时全部失效。
- `no_match` 只可短期缓存；permission/auth/protocol error 不缓存。transient error 最多
  有界重试，不能因旧错误缓存跳过恢复后的真实检索。
- 429/5xx/timeout 可按 deadline 有界重试；权限、身份和参数错误不重试、
  不切 tenant、不改走 Web。
- 先通过 shadow 指标确定 p50/p95、触发率、误触发率、无匹配率和配额，再决定 TTL
  与并发；文档不预设未经测量的数值。

## 安全边界

- 企业知识正文属于内部数据；owner-only task execution pack 可保存有界 context，
  默认日志和共享/audit receipt 只保存状态、hash、长度和 provenance，不含摘要正文。
- 检索内容按不可信外部文本处理：使用明确 delimiter 注入，不执行其中的命令、链接、
  MCP 调用或“忽略之前指令”等内容。
- 专用于 business route 的 Aily Agent 及其 MCP 必须是只读能力，关闭公开网络和所有
  写工具。否则 Agent 可能在服务端直接产生副作用，客户端不执行返回文本仍不足以
  建立安全边界。
- 交互式 business route 必须使用当前飞书请求者对应的授权身份；无人值守
  route 必须使用单独受审的 service principal。现候选仅允许固定员工交互会话；
  多用户版上线前不能共享该 UAT。
- 内部关键词未命中时不得降级 Web。若用户明确另问公开含义，应创建独立 web query，
  并在答案中与公司含义分开。
- 本地 knowledge/memory 是参考层，live runtime 和正式任务 receipt 决定当前状态。

## 分阶段接入

### A. 当前候选：显式工具

- 保留 `feishu_aily_agent_chat` 独立工具和单用户权限边界。
- 用安全 smoke 验证 Agent/UAT，不对生产任务自动触发。

### B. 统一路由 shadow

- 新增版本化 keyword registry 和 route decision。
- 只记录“本来会选择什么 route”，不实际改变回答或 RCA 结论。
- 用真实飞书问题评估漏检、误触发和延迟预算。

### C. 普通任务自动检索

- 对 required business 规则实际调用 Aily并注入有界 context。
- `no_match`/`error` 明示，禁止公网 fallback。
- 通用问题和未命中规则的问题保持原有路径。

### D. RCA shadow context

- 在正式 RCA 分支加入 host preflight 和 `knowledge_context_v1`，先随 execution request
  记录但不改变归因。
- 同时实现并审查无人值守的只读 business provider；没有可用 service identity 时记录
  `identity_unavailable`，不得借用交互式固定员工 UAT。
- 对比有/无业务上下文的责任域、abstention 和人工复核结果。

### E. RCA enforced gate

- `task_type=rca` 强制 business route。
- 只读后台身份、grounding receipt 和 `evidence_only`/abstention schema 都必须先完成；
  任一缺失时 enforced gate 保持 blocked。
- 只有证据充分的业务上下文才能支撑语义相关结论；无匹配/错误时保留数据分析，
  对业务判断 abstain。
- 经 governed release、真实 canary 和回滚演练后再启用。

## 验收场景

| 输入 | 预期 route |
| --- | --- |
| `OOI是什么?` | business required；不调用 Web。 |
| `分析这个AEB误触发RCA` | business + local required，即使正文没有更多内部关键词。 |
| `查看当前gateway为什么没重启` | local required；不默认 business/Web。 |
| `Python list append怎么用` | 不检索，直接使用通用能力。 |
| `飞书最新公开API限流是多少` | web required；若涉及本租户内部策略，再并行 business。 |
| 内部缩写未命中 | business=`no_match`；不降级 Web，不猜测。 |
| Aily 返回文本但无检索 provenance | business=`answer_only`；RCA 不得据此形成确定性业务结论。 |
| Kafka RCA 只有固定员工 UAT 可用 | business=`identity_unavailable`；不提交 VM，不借权。 |
| business 与 live 证据冲突 | 标记 `knowledge_conflict`，保留原始证据并转人工裁决。 |
