# Hermes 业务知识检索接入设计

本文只定义运行时方案：Hermes 如何像触发 Web Search 和本地知识检索一样，自动触发
飞书 Aily 企业知识问答，并把结果作为业务增强上下文接入普通任务和 RCA。

用于形成本文的盲区扫描、原型、反向采访、工作记录和验收方法，单独记录在
[设计工作记录](feishu-aily-business-integration-worklog.md)；它们不是产品状态机，
也不会成为每次 RCA 必须执行的步骤。

> 当前状态：设计候选，尚未接入生产 gateway 或正式 RCA 分支。现有 Aily Agent
> 工具只完成固定员工 UAT 的交互式候选和 API canary。

## 目标与原则

- 用户不需要显式说“查企业知识库”。内部术语、业务上下文或正式 RCA 会自动触发。
- Web、本地知识和企业知识是并列来源，不互相冒充或静默降级。
- 企业知识只帮助理解术语、预期行为、边界条件和聚焦分析方向。
- 企业知识不是原始证据，不能替代 issue、MCAP、日志、代码或 live runtime。
- 企业知识查询失败时，普通任务和 RCA 都继续原有链路，不新增业务阻断。
- 内部术语未命中时不能改用公网搜索猜测公司含义。
- 自动 RCA 使用单独受审的最小权限服务身份，绝不借用固定员工 UAT。

## 三类检索来源

| route | 触发内容 | 运行时能力 | 可信边界 |
| --- | --- | --- | --- |
| `web` | 公开且可能变化的信息、外部标准、新闻、官方公开文档 | `web_search` / `web_extract` | 外部参考，保留 URL 和时间。 |
| `local` | 代码、live runtime、任务状态、本地知识工程、历史会话 | 文件/代码搜索、受管 context retrieval、`session_search` | live 证据优先，wiki/memory 仍是参考层。 |
| `business` | 公司术语、产品行为、业务规则、内部流程、项目/RCA 语义 | Aily business knowledge provider | 内部参考知识，不是执行真相。 |

一个任务可以同时选择多个 route。例如 RCA 默认需要 local/live 证据，也尝试获取
business 上下文；只有问题确实涉及公开标准时才额外选择 Web。

## 机器契约

`required` 表示必须**尝试** business lookup 并记录结果，不表示检索成功是任务继续的
门禁。RCA 的失败策略固定为 `continue_original_chain`。

<!-- knowledge-routing-contract:begin -->
```json
{
  "requirements": ["none", "auto", "required"],
  "statuses": [
    "grounded_match",
    "answer_only",
    "no_match",
    "identity_unavailable",
    "timeout",
    "error",
    "not_required"
  ],
  "influences": ["observe_only", "reference_only", "none"],
  "canonical_json_v1": {
    "encoding": "utf-8",
    "ensure_ascii": false,
    "sort_keys": true,
    "separators": [",", ":"],
    "allow_nan": false
  },
  "effective_influence_by_mode": {
    "shadow": {
      "grounded_match": "observe_only",
      "answer_only": "observe_only",
      "no_match": "none",
      "identity_unavailable": "none",
      "timeout": "none",
      "error": "none",
      "not_required": "none"
    },
    "active": {
      "grounded_match": "reference_only",
      "answer_only": "reference_only",
      "no_match": "none",
      "identity_unavailable": "none",
      "timeout": "none",
      "error": "none",
      "not_required": "none"
    }
  },
  "enhancement_only": true,
  "gates_original_chain": false,
  "failure_policy": "continue_original_chain",
  "business_knowledge_is_execution_evidence": false,
  "original_chain_mutations": {
    "dispatcher": false,
    "execution_request_v2": false,
    "vm_goal": false,
    "core_result": false,
    "required_delivery": false
  },
  "design_method_runtime": false,
  "lookup_unique_key": [
    "submission_key",
    "generation",
    "phase",
    "query_hmac_sha256",
    "provider_policy_fingerprint",
    "identity_policy_fingerprint"
  ],
  "rca_enhancement_points": [
    "post_vm_materialization_async_preflight",
    "post_core_gap_async_lookup",
    "owner_only_reference_appendix"
  ],
  "second_stage_fork": {
    "after": "s6_report_seal_and_optional_gap_attempt",
    "main_branch": [
      "main_task_completion_without_host_wait",
      "original_required_delivery"
    ],
    "reference_branch_requires": "valid_gap_atomic_seal",
    "reference_branch": [
      "host_gap_observer_claim",
      "host_business_lookup_create_once",
      "owner_only_addendum_seal"
    ]
  },
  "second_stage_gap_outcomes": {
    "valid_gap": ["main_branch", "reference_branch"],
    "gap_absent": ["main_branch"],
    "gap_build_or_seal_error": ["main_branch", "knowledge_local_metric_only"]
  },
  "second_stage_controls": {
    "vm_direct_access": false,
    "same_generation_binding": true,
    "cross_generation_reuse": false,
    "human_blocking_state": false,
    "main_task_resume": false,
    "required_delivery_effect": false,
    "gap_artifact_failure_blocks_main": false,
    "max_rounds": 1,
    "max_queries": 2
  },
  "rca_provider_session_policy": {
    "create_request_session_id": null,
    "fresh_session_per_job": true,
    "cross_job_session_reuse": false
  },
  "lookup_receipt_common_required": [
    "schema_version",
    "submission_key",
    "generation",
    "phase",
    "mode",
    "status",
    "influence",
    "rca_contract_sha256",
    "query_hmac_sha256",
    "provider_policy_fingerprint",
    "identity_policy_fingerprint",
    "retrieved_at",
    "latency_ms"
  ],
  "lookup_receipt_status_fields_exact": {
    "grounded_match": [
      "answer_sha256",
      "answer_bytes",
      "source_refs_relpath",
      "source_refs_sha256",
      "retrieval_activity_receipt_relpath",
      "retrieval_activity_receipt_sha256"
    ],
    "answer_only": ["answer_sha256", "answer_bytes"],
    "no_match": [
      "provider_no_hit_receipt_relpath",
      "provider_no_hit_receipt_sha256"
    ],
    "identity_unavailable": ["error_code"],
    "timeout": ["error_code"],
    "error": ["error_code"]
  },
  "failure_statuses": ["identity_unavailable", "timeout", "error"],
  "consumer_receipt_binding_required": [
    "lookup_receipt_relpath",
    "lookup_receipt_sha256"
  ],
  "consumer_lookup_fields_must_equal": [
    "submission_key",
    "generation",
    "phase",
    "mode",
    "status",
    "influence",
    "rca_contract_sha256",
    "query_hmac_sha256",
    "answer_sha256",
    "answer_bytes",
    "provider_policy_fingerprint",
    "identity_policy_fingerprint"
  ],
  "consumer_content_binding": {
    "business_knowledge_context_v1": "answer",
    "business_knowledge_addendum_v1": "content",
    "encoding": "utf-8",
    "length_field": "answer_bytes",
    "sha256_field": "answer_sha256"
  },
  "shared_receipt_common_fields_exact": [
    "schema_version",
    "submission_key",
    "generation",
    "phase",
    "mode",
    "status",
    "influence",
    "query_hmac_sha256",
    "provider_policy_fingerprint",
    "identity_policy_fingerprint",
    "lookup_receipt_sha256",
    "retrieved_at",
    "latency_ms"
  ],
  "shared_receipt_status_fields_exact": {
    "grounded_match": ["answer_bytes"],
    "answer_only": ["answer_bytes"],
    "no_match": [],
    "identity_unavailable": ["error_code"],
    "timeout": ["error_code"],
    "error": ["error_code"]
  },
  "shared_receipt_forbidden_fields": [
    "query",
    "summary",
    "content",
    "raw_answer",
    "open_id",
    "union_id",
    "token",
    "agent_chat_id",
    "session_id",
    "lookup_receipt_relpath",
    "source_refs_relpath",
    "retrieval_activity_receipt_relpath",
    "provider_no_hit_receipt_relpath"
  ]
}
```
<!-- knowledge-routing-contract:end -->

状态只描述增强结果，不改变原任务 disposition：

| status | 可用方式 | 原任务 disposition |
| --- | --- | --- |
| `grounded_match` | 可生成 `reference_only` 业务参考，仍不是执行证据。 | continue |
| `answer_only` | 可生成显式“未验证业务参考”，只提示方向，不当成已证实事实。 | continue |
| `no_match` | 不注入企业事实，不 fallback Web。 | continue |
| `identity_unavailable` | 不借其他员工、tenant 或其他 Agent 身份。 | continue |
| `timeout` / `error` | 只进入增强自身指标和 safe receipt。 | continue |
| `not_required` | 不执行 business provider。 | continue |

## 无感触发

### 确定性路由

路由发生在主模型开始推理前，先使用本地、版本化规则，不额外调用大模型：

1. `task_type=rca` 或正式 RCA intake：`business=required`。
2. 命中内部缩写、功能名、车型/项目代号、内部字段、组织流程或内部域名：
   `business=auto`。
3. 当前代码、runtime、日志和配置问题：`local=required`；出现业务术语时并行 business。
4. 公开时效问题：`web=required`；同时出现内部术语时拆成 business 与 web 两个查询。
5. 纯通用编程、数学或语言转换：business=`none`。

RCA 是否尝试检索由任务类型决定，关键词只决定“查什么”。因此 issue 标题没有显式
业务缩写也不会漏掉 business route。

### 关键词注册表

规则存放在版本化、可评审的配置中，不放 `.env`：

```yaml
id: pnc-ooi
patterns: [OOI]
aliases: [关注目标, 感兴趣目标]
domains: [pnc, planning, rca]
query_kinds: [term_definition, expected_behavior, abnormal_boundary]
```

- ASCII 缩写按 token/词边界匹配，短词必须同时命中 domain 上下文。
- 中文别名做规范化精确匹配，不使用无界模糊子串。
- 注册表只保存词、domain、查询模板和版本，不保存知识正文、用户 ID 或 secret。
- 内部词未命中 business 时不 fallback Web；公开同名概念必须作为独立 web query。

## 普通飞书任务

在飞书 session identity 已绑定、首轮主模型调用开始前执行 route decision：

1. route 未命中：完全保持原行为。
2. route 命中且当前用户有受审 UAT：执行至多一条有界 business query。
3. 将 `grounded_match` 或 `answer_only` 作为带边界的 `business_reference` 注入主任务；
   后者必须显式标记“未验证业务参考”，两者都不是执行证据。
4. `no_match`、身份不可用、超时或错误：不注入答案，主任务继续。
5. 只有用户的问题明确依赖内部含义而检索失败时，最终答复才说明“企业知识未验证”；
   不为每次后台检索展示工具过程。

当前固定员工 UAT 仍只允许对应飞书会话使用。多人场景必须采用逐用户 OAuth、稳定
身份映射、撤权和并发隔离，不能共享该 UAT。

## RCA 首轮增强

### 插入位置

首版不修改 host outbox dispatcher。独立 observer 在 VM task 成功物化后，只读 sealed
goal 和最终 v2 request，再异步执行企业知识增强：

```text
durable admission
  -> claim
  -> official issue preread / enrich
  -> original v2 request / reservation / VM submit
  -> original VM core / report / required delivery

independent host coordinator
  -> observe materialized VM task read-only
  -> async preflight business lookup
  -> observe optional completed core gap artifact
  -> async supplemental lookup
  -> owner-only reference appendix + safe receipt
```

Observer 复用 task-owned 定位符读取已经封存的 request，不向 control DB、outbox、VM
goal 或原 delivery store 写入。Aily create/poll 不占用 outbox lease，也不能打开原
dispatcher circuit。不能把网络调用放入 Kafka consumer、SQLite transaction、飞书
callback、VM、报告渲染或投递阶段。

这意味着首版 RCA 增强是独立参考 lane：它能给人工复核、后续追问或下一次受控分析
提供术语和聚焦方向，但不会改变当前一次性 VM core。若未来要求 Aily 参与当前 core
的 hypothesis/evaluator 顺序，必须另行设计 staged VM workflow；当前 sealed fixed-goal
任务没有可安全 resume 的 pre-render 阶段。

### 查询范围

首轮只使用经过 allowlist 的最小字段：

- `business_profile.profile_id` 和版本；
- `function_category` / `function_domain`；
- title 中命中的注册术语和有界业务摘要。

默认禁止发送 work item ID/URL、负责人、车辆信息、PDCL 地址、frame、完整描述、评论、
附件和人工填写的根因。业务查询只问：

1. 术语在当前业务域的定义；
2. 预期状态或目标切换行为；
3. 正常/异常边界以及不能单独作为异常依据的现象。

### 对 RCA 的影响

原 VM core 不读取 Aily，先独立封存原始证据和 core result。成功上下文只能进入独立
reference appendix：

- 展开术语；
- 提示应关注的信号和时间窗口；
- 提供待验证的业务假设和后续聚焦方向；
- 为人工复核或后续追问提供正确业务措辞。

它不能直接将某个模块定为根因、证明责任归属、覆盖 evaluator 输出或把任务标记完成。
所有业务假设仍必须由原始数据、代码或 live 证据独立验证。

检索失败时写入脱敏状态并继续原链。`identity_unavailable`、`no_match`、`timeout`、
`error` 和只有文本但缺少 provenance 的 `answer_only` 都不修改 core result，也不是
VM submit、原报告或投递门禁。只有 `grounded_match` 或明确标记的 `answer_only` 才能
生成 owner-only 独立参考附录；其他状态没有附录，原输出始终保持原行为。首版不自动
把附录写回飞书；后续若需要通知，必须使用独立 best-effort store/circuit，不能复用
required delivery。首版附录通过 host-private、task-bound artifact 和后续 Hermes 追问读取。

## 二阶段业务补查

首版契约预留一次 host-mediated 补查，用于 VM core 结果中才暴露的内部术语或业务
盲点。VM 始终不持有 UAT，也不直接访问 Aily；它只额外产出有界 gap artifact，随后
继续并完成原任务，不等待知识结果。

```text
VM core
  -> seal original S6 report
  -> best-effort optional business_knowledge_gap_v1 atomic seal
  +-> main branch: complete original task and required delivery without host wait

  +-> valid-gap-only reference branch: host observer validates and claims gap
        -> host performs create-once lookup
        -> seals owner-only business_knowledge_addendum_v1
```

`business_knowledge_gap_v1` 只允许：

```json
{
  "schema_version": "business_knowledge_gap_v1",
  "submission_key": "...",
  "generation": 1,
  "phase": "gap:1",
  "gap_id": "...",
  "run_id": "...",
  "artifact_set_id": "...",
  "request_sha256": "...",
  "rca_contract_sha256": "...",
  "s6_stage_receipt_sha256": "...",
  "round": 1,
  "terms": ["OOI"],
  "question_kind": "abnormal_boundary",
  "reason_code": "unknown_business_semantics",
  "context_hint": "目标选择状态切换的业务边界不明确",
  "evidence_locator_refs": ["signal-window:target-selection"]
}
```

约束：

- 首版最多一轮、最多两条 query；同一 `phase` 内相同 query digest 不重复调用。
- gap 不能包含原始帧、日志正文、附件、PDCL、用户信息或自定义 prompt 指令。
- gap 本地生成、校验或 seal 失败时直接跳过 reference branch，主 task completion 和
  required delivery 继续；不得把 gap 变成 S6、task completion 或 delivery gate。
- VM helper 必须捕获 gap 的 ENOSPC、permission、schema/hash、existing-target conflict 和
  fsync 错误，写有界 knowledge-local metric 后返回原 fixed CLI disposition；禁止让这些
  异常改变原 exit code、result/report 或 required delivery。
- host 重新按注册表和业务 profile 构造最终问题，不能原样执行 VM 文本。
- lookup job/receipt 无论成功或失败都是终态；只有 `grounded_match`/`answer_only` 生成
  addendum，同一 hash 不允许形成查询循环。
- addendum 绑定同一 `submission_key`、generation、RCA contract 和 S6 stage receipt，但不
  resume、不改写也不重新投递主 RCA。
- 补查失败时只记录“本轮业务增强不可用”，原 RCA 无需恢复，因为从未等待它。
- 第二阶段默认 feature flag 关闭；完成幂等、artifact lineage 和故障注入测试后再开启。

字段上限：`terms<=8`、`context_hint<=400`、`evidence_locator_refs<=8`。正式实现必须在
`gateway/pnc_rca_stage_lineage.py` 暴露并由 VM/host 共用版本化
`business_knowledge_canonical_json_v1` helper，其行为严格等于机器契约中的 UTF-8、
`ensure_ascii=false`、sorted keys、compact separators、禁止 NaN；不得分别调用默认
`json.dumps`。`gap_id` 是以该 helper 编码“排除 `gap_id` 后的 normalized gap”再取
SHA-256。VM 只可在 S6 receipt 已封存后，将 gap 写到
`<artifact_root>/business_knowledge/gaps/gap-1.json`；其中 `artifact_root` 必须是现有
RCA contract 的 `/mnt/tmp/<submission_key>/`。`rca_contract_sha256` 必须等于
`vm_task_status(...).meta.rca_contract_sha256`。observer 必须先用
`validate_stage_lineage_chain` 校验完整 S3A->S6 hash chain、expected finished timestamps、
当前 required final outputs，以及 S6 identity 的 `task_id/submission_key/run_id/artifact_set_id/
request_sha256/rca_contract_sha256` 六个字段全部等于当前 task/goal 的 expected identity；只调
`validate_stage_lineage_receipt` 不够。`s6_stage_receipt_sha256` 是对 full-chain validator
返回的 normalized S6 receipt 使用同一 helper 计算的 canonical JSON SHA-256，对应文件为
`<artifact_root>/stage_lineage/s6_report.json`。现有
`rca_contract_sha256` 则直接与 task meta 的既有值比较，不能用新 helper 重算。

gap 使用同目录 `0600` 临时文件，完成 bounded JSON、owner、regular-file、no-symlink、
size 和 hash 校验后，以 no-replace 的同文件系统原子 seal 写入最终路径并 `fsync` 父目录；
已存在目标只能按 hash 幂等读取，不能覆盖。该文件不得进入 `delivery_contract.json`、
`delivery_manifest.json` 或 required delivery artifact 集。observer 只接受已完成 task、
generation、RCA contract 和 S6 receipt 全部匹配的 gap；迟到或不匹配的结果只能写
knowledge-local stale receipt。

Owner-only addendum 使用 exact schema：

```json
{
  "schema_version": "business_knowledge_addendum_v1",
  "mode": "active",
  "submission_key": "...",
  "generation": 1,
  "phase": "gap:1",
  "gap_id": "...",
  "run_id": "...",
  "artifact_set_id": "...",
  "request_sha256": "...",
  "rca_contract_sha256": "...",
  "s6_stage_receipt_sha256": "...",
  "status": "answer_only",
  "influence": "reference_only",
  "query_hmac_sha256": "...",
  "answer_sha256": "656584fa205311b0846a70271f1778fce74d0990a9992967a1288040ec68c04c",
  "answer_bytes": 33,
  "provider_policy_fingerprint": "...",
  "identity_policy_fingerprint": "...",
  "lookup_receipt_relpath": "gap-1/<job_id>/lookup-receipt.json",
  "lookup_receipt_sha256": "...",
  "content": "owner-only bounded reference text"
}
```

Observer 对 `(submission_key,generation,phase,gap_id)` 做原子 claim/CAS；同一 key 只有
一个 worker 可 create。每 generation 的 ledger 最多 `1` 条 preflight 和 `2` 条 gap
query，create 总数不超过 `3`。重复 gap、继续 poll 和读取已完成 context 不消耗新额度。

## Provider 与身份

交互式和后台 provider 必须分离：

| 场景 | 身份 | 边界 |
| --- | --- | --- |
| 普通飞书任务 | 当前请求用户的 UAT | 仅该用户会话；按用户权限过滤。 |
| Kafka/outbox RCA | 专用 RCA 服务用户/UAT | 无人值守、最小知识范围、可轮换、可审计。 |

后台服务用户只允许访问已批准可用于 RCA 报告受众的知识范围，不能凭服务账号扩大
最终接收者原本不应看到的内容。专用 Aily Agent 必须：

- 只启用企业知识检索；
- 关闭公开网络；
- 不绑定写型 MCP、飞书写工具或其他有副作用技能；
- 发布版本、知识范围和工具策略可形成 fingerprint；
- 调用应用和服务用户在 Aily OpenAPI 用户范围内。

如果后台身份或等价知识范围尚未就绪，provider 返回 `identity_unavailable`，RCA
继续原链，绝不能回退固定员工 UAT、tenant identity、Web 或另一个 Agent。

## 结果与持久化

每次 RCA provider job 都使用 active governed Hermes home 的 host 私有根
`$HERMES_HOME/runtime/rca-prod/business-knowledge/<submission_key>/<generation>/`
持久化状态。POST 前先 durable seal `creating` job state；获得 terminal status 后、任何
消费者读取前，再封存 owner-only `business_knowledge_lookup_receipt_v1`。`phase_path` 只能是
`preflight` 或 `gap-<round>`，`job_id` 是机器契约中六元 create-once key 的 canonical
JSON SHA-256；因此最终 receipt 路径固定为
`<phase_path>/<job_id>/lookup-receipt.json`。目录为 `0700`、文件为 `0600`，使用与 gap
相同的 bounded、no-symlink、no-replace、fsync 原子 seal 规则。

`answer_only` receipt 示例：

```json
{
  "schema_version": "business_knowledge_lookup_receipt_v1",
  "submission_key": "...",
  "generation": 1,
  "phase": "preflight",
  "mode": "active",
  "status": "answer_only",
  "influence": "reference_only",
  "rca_contract_sha256": "...",
  "query_hmac_sha256": "...",
  "answer_sha256": "656584fa205311b0846a70271f1778fce74d0990a9992967a1288040ec68c04c",
  "answer_bytes": 33,
  "provider_policy_fingerprint": "...",
  "identity_policy_fingerprint": "...",
  "retrieved_at": "...",
  "latency_ms": 0
}
```

状态字段是条件合同，不允许只改 `status` 标签：

receipt 顶层字段集合必须严格等于 `lookup_receipt_common_required` 与对应
`lookup_receipt_status_fields_exact[status]` 的并集；未知字段、`answer`、`content`、
`raw_answer`、内联 source/activity/no-hit payload 一律拒绝。

- `grounded_match` 还必须包含 `source_refs_relpath/source_refs_sha256` 和
  `retrieval_activity_receipt_relpath/retrieval_activity_receipt_sha256`；两个相对路径
  都必须位于同一 job 目录，且目标已按同一规则不可变封存。
- `answer_only` 必须包含 answer hash 和正数 `answer_bytes`，但不得包含 source/no-hit 字段。
- `no_match` 必须包含 `provider_no_hit_receipt_relpath/provider_no_hit_receipt_sha256`，
  且不得包含 answer/content/source 字段。
- `identity_unavailable`、`timeout`、`error` 必须包含 bounded `error_code`，只允许安全诊断，
  不得包含 answer/content/source/no-hit 字段；这些状态以 lookup receipt 终止且不创建
  context/addendum。`not_required` 不创建 provider job。

生产 coordinator 必须绑定 reviewed active home，不能接受每个 job 的 `HERMES_HOME` 或
状态根覆盖。所有 `*_relpath` 都以 generation 根为基准，规范化后不得绝对化、穿越父目录
或指向符号链接。

当前 Agent Chat 无稳定 source/activity/no-hit artifact，因此只能产生 `answer_only` 或失败
状态。只有 owner-only lookup receipt 通过 schema、条件字段、路径和 hash 校验后，消费者
才可生成 context/addendum；共享 safe receipt 只记录其 SHA-256，不记录私有路径或正文。

成功返回正文的查询再生成 owner-only `business_knowledge_context_v1`：

```json
{
  "schema_version": "business_knowledge_context_v1",
  "mode": "active",
  "status": "answer_only",
  "influence": "reference_only",
  "submission_key": "...",
  "generation": 1,
  "phase": "preflight",
  "rca_contract_sha256": "...",
  "query_kind": "expected_behavior",
  "query_hmac_sha256": "...",
  "answer_sha256": "656584fa205311b0846a70271f1778fce74d0990a9992967a1288040ec68c04c",
  "answer_bytes": 33,
  "provider_policy_fingerprint": "...",
  "identity_policy_fingerprint": "...",
  "lookup_receipt_relpath": "preflight/<job_id>/lookup-receipt.json",
  "lookup_receipt_sha256": "...",
  "retrieved_at": "...",
  "latency_ms": 0,
  "answer": "owner-only bounded reference text"
}
```

消费者必须重新读取并校验 immutable lookup receipt，然后要求
`consumer_lookup_fields_must_equal` 中每个字段逐值相等。再对 context 的 `answer` 或
addendum 的 `content` 取原始 UTF-8 bytes，要求 `len(bytes)==answer_bytes` 且
`sha256(bytes)==answer_sha256`；任一不一致都丢弃 reference artifact、记录 knowledge-local
integrity error，并继续原任务。只校验 `lookup_receipt_sha256` 而不校验正文绑定不合格。

Shared/audit 层使用 exact `business_knowledge_safe_receipt_v1`，例如：

```json
{
  "schema_version": "business_knowledge_safe_receipt_v1",
  "submission_key": "...",
  "generation": 1,
  "phase": "preflight",
  "mode": "active",
  "status": "answer_only",
  "influence": "reference_only",
  "query_hmac_sha256": "...",
  "provider_policy_fingerprint": "...",
  "identity_policy_fingerprint": "...",
  "lookup_receipt_sha256": "...",
  "retrieved_at": "...",
  "latency_ms": 0,
  "answer_bytes": 33
}
```

Safe receipt 顶层字段集合必须严格等于 `shared_receipt_common_fields_exact` 与对应
`shared_receipt_status_fields_exact[status]` 的并集；未知 key 立即拒绝。该 allowlist 是
主安全合同，`shared_receipt_forbidden_fields` 只是 defense-in-depth 检查。

- `(submission_key, generation, phase, query_hmac_sha256,
  provider_policy_fingerprint, identity_policy_fingerprint)` 是唯一 create-once key。
  `phase` 只能是 `preflight`
  或有界的 `gap:<round>`；首轮和二阶段不会因文本相同而错误共用 job。
- create 已成功但 poll 结果不确定时不得自动创建第二个 chat。正式后台 provider 必须把
  create 与 poll 拆成两个可恢复操作，或在收到 chat ID 后先调用 durable callback 封存
  `chat_created` 再开始 poll；现有一次性 `run_agent_chat_user()` 不能原样承担该状态机。
- RCA create body 必须省略 `session_id`，每个六元 job 获得全新 Aily session。返回的
  session/chat ID 只可在该 owner-only job 内用于恢复 poll，绝不能用于另一个 job、task
  或 generation；现有 transport 的可选 `session_id` 参数在后台 provider 路径必须固定为
  `None`。
- owner-only context 权限为 `0600`，有正文大小和保留期上限。
- shared/audit receipt 只保留 exact allowlist 中的状态、长度、HMAC、策略版本、耗时和
  时间，不保存查询、
  答案、个人标识、chat/session ID 或凭据。
- 低熵缩写在共享回执中使用 keyed HMAC，避免裸 SHA 被字典反推。
- 不跨 generation 或授权主体复用答案；同 generation 仅用于幂等去重。
- `identity_policy_fingerprint` 绑定稳定服务主体、auth mode 和已批准知识范围，不含 token、
  open/union ID 明文；身份或 ACL 变化必须产生新 job，不能命中旧结果。
- effective influence 只按机器契约的完整矩阵计算：active 下
  `grounded_match/answer_only -> reference_only`，shadow 下这两种状态只能
  `observe_only`；所有其他状态在两种模式下都是 `none`。context/addendum 的 mode、status
  和 influence 必须与 lookup receipt 完全一致，shadow 结果不能生成面向任务消费者的
  reference appendix。

知识 job 使用单调状态：

```text
reserved -> identity_check -> identity_unavailable
                           -> creating -> chat_created -> polling
         -> grounded_match | answer_only | no_match
         | timeout | error | create_unknown
```

上面是 provider 内部 `job_state`，不是对外 `status`。POST 前先持久化 `creating`。
崩溃恢复时若没有已封存的 chat ID，则 job 转 `create_unknown`，对外映射为
`status=error` 并结束本次增强，不能盲目再次 POST；已知 chat ID 时只允许继续 GET
poll。该保守策略可能丢失一次增强，但不会重复执行服务端 Agent，也不影响原 RCA。
`identity_unavailable` 是 create 前的独立 terminal job state，并映射同名对外 status；
`not_required` 在路由层结束，不创建 job。

`no_match` 只能来自绑定 query、provider policy 和本次调用的结构化 no-hit receipt。
当前 Agent 文本自行声称“未检索到”不满足该条件，应保持 `answer_only`；不能把模型
措辞提升为 provider no-hit 事实。

当前 Agent Chat 只有 `Completed`、文本、chat/session 和 artifact 标识，没有稳定来源
契约。因此默认只能标为 `answer_only`。只有同时绑定 query、answer、source refs、
Agent/知识策略版本和本次活动 receipt 时，才允许标 `grounded_match`。

## 失败与隔离

- business lookup 使用独立限流、deadline、熔断和指标，不打开原 RCA dispatcher circuit。
- lookup 不占用现有 dispatcher 长 lease；正式实现采用独立 durable enrichment stage。
- provider 返回文本按不可信输入处理，用 delimiter 隔离，不能执行其中的命令、链接、
  MCP 指令或“忽略之前规则”。
- business 与 local/live 冲突时保留 `knowledge_conflict`，以原始证据为准。
- Aily 超时、限流或停机不能改变原 execution request、canonical hash、VM submit、报告
  和投递结果；除新增可观察 receipt 外，禁用增强应能恢复原行为。

## 最小可逆原型

第一阶段不是直接修改 dispatcher，而是独立只读 shadow observer：

1. 读取已完成 RCA 的只读 execution request/goal 定位符；
2. 运行同一确定性路由和 query builder；
3. 使用专用只读 provider 或 mock；
4. 写 owner-only context 和脱敏 receipt；
5. 对比开启/关闭增强时的术语覆盖、分析聚焦度和误导率；
6. 不修改 outbox、VM、报告、投递或 production circuit。

停止 observer 即完整回滚。Shadow 验收先证明身份、知识范围、grounding 能力、延迟和
无副作用，再扩展为只读跟踪新物化 task 的 live reference observer。

## 风险优先实施顺序

1. **身份与 ACL**：建立专用服务用户、最小 RCA 知识范围和输出受众规则。
2. **Agent 策略**：证明无公网、无写工具，并固化发布策略 fingerprint。
3. **Provider receipt**：验证是否能获得来源/活动证据；不能则明确保持 `answer_only`。
4. **Shadow observer**：create-once、限流、脱敏、故障隔离和对照评估。
5. **普通飞书自动路由**：关键词命中自动预取，失败继续原任务。
6. **RCA live reference lane**：只读跟踪新物化 task，不改 dispatcher、v2 request、
   core result 或 required delivery。
7. **二阶段补查**：先完成 gap/addendum artifact lineage，再打开一轮 feature flag；
   主 task 不等待、不 resume。
8. **受管发布**：正式 RCA 分支回归、materialize、gateway restart、readback 和 canary。

## 验收场景

| 场景 | 预期 |
| --- | --- |
| `OOI是什么?` | business auto；不调用 Web。 |
| 正式 AEB RCA，标题无缩写 | business required attempt + local；原 RCA 始终继续。 |
| Aily timeout / 403 / no-match | 记录状态；VM、报告和投递保持原行为。 |
| 返回非空文本但无来源 | `answer_only/reference_only`，不能作为根因证据。 |
| 通用 Python 问题 | 不触发 business。 |
| 内部词和公开标准混合 | business 与 web 分开查询，不互相 fallback。 |
| Kafka RCA 只有固定员工 UAT | `identity_unavailable`，不用该 UAT，原 RCA 继续。 |
| VM 新发现业务术语 | 最多一轮 host gap、两条 lookup；主 task 不等待，addendum 绑定原 generation。 |
| business 与 live 证据冲突 | 标记冲突，以 live/原始证据为准。 |

后续正式分支的文件、测试和发布交接见
[集成交接](feishu-aily-business-integration-handoff.md)。
