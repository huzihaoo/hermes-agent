# Hermes 业务知识检索接入设计

本文只定义运行时方案：Hermes 如何像触发 Web Search 和本地知识检索一样选择飞书 Aily
企业知识问答。首版执行面只接入本机 RCA observer；普通任务路由保留为后续设计语义，
不获得胡子豪 UAT 或后台 provider 调用能力。

用于形成本文的盲区扫描、原型、反向采访、工作记录和验收方法，单独记录在
[设计工作记录](feishu-aily-business-integration-worklog.md)；它们不是产品状态机，
也不会成为每次 RCA 必须执行的步骤。

2026-08-22 owner 已允许将 exact candidate 源码合入正式 Host；这不改变本文的
运行时合同。Aily toolsets/provider/consumer seam 仍 default-off，凭据和状态仍只能位于
owner-only 本机边界，源码存在不是启用证据。

> 当前状态：本机已有手动、一次性的 shadow observer 原型
> `~/.hermes/scripts/pnc_rca_aily_shadow_observer.py` 和入口
> `~/bin/pnc-rca-aily-shadow`，但它未启用、未注册、不是 daemon，也没有自动 consumer。
> `inspect` 和 `--provider dry-run` 不联网；真实 provider 当前禁用且不可用。固定 UAT 的
> standalone canary 只证明 transport 能力，不证明 observer 已帮助任何 RCA。

## 目标与原则

- 只有版本化注册表命中的业务术语，或通过 stdin 提供的显式有界业务查询，才触发
  business route；`task_type=rca` 本身不触发。
- Web、本地知识和企业知识是并列来源，不互相冒充或静默降级。
- 企业知识只帮助理解术语、预期行为、边界条件和聚焦分析方向。
- 企业知识不是原始证据，不能替代 issue、MCAP、日志、代码或 live runtime。
- 企业知识查询失败时，普通任务和 RCA 都继续原有链路，不新增业务阻断。
- 内部术语未命中时不能改用公网搜索猜测公司含义。
- 首版无人值守 RCA 以能力可用为先，在本机 Hermes host 复用预登记的胡子豪固定 UAT；
  专用 service identity 和身份借用风险专项明确后置，不作为首版门禁。
- 该固定 UAT 只供独立 RCA host observer/provider 使用，不开放为任意会话可调用的通用
  借权能力，也不进入 VM、业务 schema 或正式 Host 源码配置。default-off 代码可存在
  于 Host 树，但不包含 UAT 或运行时授权。

## 三类检索来源

| route | 触发内容 | 运行时能力 | 可信边界 |
| --- | --- | --- | --- |
| `web` | 公开且可能变化的信息、外部标准、新闻、官方公开文档 | `web_search` / `web_extract` | 外部参考，保留 URL 和时间。 |
| `local` | 代码、live runtime、任务状态、本地知识工程、历史会话 | 文件/代码搜索、受管 context retrieval、`session_search` | live 证据优先，wiki/memory 仍是参考层。 |
| `business` | 公司术语、产品行为、业务规则、内部流程、项目/RCA 语义 | Aily business knowledge provider | 内部参考知识，不是执行真相。 |

一个任务可以同时选择多个 route。RCA 默认需要 local/live 证据；只有确定性业务信号
命中时才额外选择 business。无注册术语且无显式查询时不调用 Aily，也不为内部含义
降级到 Web，原 RCA 继续。

## 机器契约

`required` 只表示确定性触发信号已经命中后必须尝试 business lookup；它不由 RCA 类型
无条件产生，也不表示检索成功是任务继续的门禁。无信号规范化为 `not_required`，手动
原型可显示别名 `not_triggered`；两者都不创建 provider job。RCA 的失败策略固定为
`continue_original_chain`。

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
  "rca_contract_hash_json_v1": {
    "purpose": "canonical hash of the sealed RCA admission/request projection",
    "encoding": "utf-8",
    "ensure_ascii": true,
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
  "local_host_only": true,
  "business_repo_changes": false,
  "vm_changes": false,
  "credentials_and_provider_host_only": true,
  "owner_only_reference": true,
  "web_fallback_for_business": false,
  "identity_strategy": "reuse_pre_registered_huzhihao_fixed_uat",
  "capability_priority": true,
  "risk_deferred": true,
  "deferred_items": [
    "dedicated_service_identity",
    "identity_and_delegation_risk_review"
  ],
  "identity_controls": {
    "pre_registered_owner": "胡子豪",
    "exact_match_required": ["profile", "open_id", "union_id"],
    "verify_before_each_create": true,
    "credential_broker": "keychain_lark_cli",
    "raw_token_export": false,
    "token_to_vm": false,
    "rca_host_observer_provider_only": true,
    "general_session_delegation": false
  },
  "initial_runtime_scope": "rca_local_host_observer_provider_only",
  "ordinary_task_provider_enabled": false,
  "trigger_policy": {
    "type": "deterministic_registered_term_or_explicit_query",
    "signals": ["registered_business_term", "explicit_bounded_query"],
    "rca_task_type_alone_triggers": false,
    "no_signal_requirement": "none",
    "no_signal_status": "not_required",
    "prototype_status_alias": "not_triggered",
    "provider_called_without_signal": false,
    "web_fallback_without_signal": false
  },
  "manual_shadow_prototype": {
    "script": "~/.hermes/scripts/pnc_rca_aily_shadow_observer.py",
    "wrapper": "~/bin/pnc-rca-aily-shadow",
    "execution": "manual_one_shot",
    "enabled": false,
    "registered": false,
    "daemon": false,
    "inspect_network": false,
    "dry_run_network": false,
    "real_provider_enabled": false,
    "durable_create_poll_recovery": false,
    "consumer_seam_enabled": false
  },
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
    "post_vm_materialization_host_async_preflight",
    "post_completed_sealed_report_host_async_followup",
    "host_private_owner_only_reference"
  ],
  "preflight_task_states": ["pending", "claimed", "running", "in_progress", "completed", "done", "closed"],
  "report_followup_task_states": ["completed", "done", "closed"],
  "report_followup_requires_sealed_delivery": true,
  "rca_phases": ["preflight", "report_followup:1"],
  "second_stage_host_observer": {
    "trigger": "completed_sealed_report",
    "source_access": "read_only",
    "term_extraction": "deterministic_registry",
    "provider_execution": "local_host_only",
    "output": "host_private_owner_only_addendum",
    "original_task_wait": false
  },
  "second_stage_controls": {
    "sealed_report_required": true,
    "same_generation_binding": true,
    "cross_generation_reuse": false,
    "human_blocking_state": false,
    "main_task_wait": false,
    "main_task_resume": false,
    "required_delivery_effect": false,
    "business_schema_write": false,
    "report_observation_failure_blocks_main": false,
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
  "host_audit_receipt_common_fields_exact": [
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
  "host_audit_receipt_status_fields_exact": {
    "grounded_match": ["answer_bytes"],
    "answer_only": ["answer_bytes"],
    "no_match": [],
    "identity_unavailable": ["error_code"],
    "timeout": ["error_code"],
    "error": ["error_code"]
  },
  "host_audit_receipt_forbidden_fields": [
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
| `identity_unavailable` | 预登记的胡子豪 profile/open_id/union_id 无法精确验证；不换用其他身份。 | continue |
| `timeout` / `error` | 只进入增强自身指标和 safe receipt。 | continue |
| `not_required` | 无确定性信号，不执行 business provider；手动原型可显示 `not_triggered`。 | continue |

## 无感触发

### 确定性路由

路由发生在主模型开始推理前，先使用本地、版本化规则，不额外调用大模型：

1. 命中版本化注册表中的内部缩写、功能名、车型/项目代号、内部字段或中文别名：
   `business=required`。
2. owner 通过 stdin 提供显式、有界的业务查询：`business=required`。
3. 无注册术语且无显式查询，包括仅有 `task_type=rca`：`business=none`，规范状态为
   `not_required`，手动原型可显示 `not_triggered`；不调用 Aily。
4. 当前代码、runtime、日志和配置问题：`local=required`；只有同时命中上述确定性业务
   信号时才并行 business。
5. 公开时效问题：`web=required`；同时命中注册业务术语时拆成 business 与 web 两个
   独立查询，二者不互相 fallback。

任务类型只确定这是一个可被本机 observer 检查的 RCA，不足以触发企业知识查询。首版
随后还执行 runtime scope gate：只有本机 RCA observer 能进入 provider；普通任务的
business 语义是未来设计，当前一律映射为 `not_required`。

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

## 普通飞书任务（首版不启用）

以下是后续逐用户能力的保留语义，不属于首版运行合同：

1. route 未命中：完全保持原行为。
2. route 命中且当前用户有受审 UAT：执行至多一条有界 business query。
3. 将 `grounded_match` 或 `answer_only` 作为带边界的 `business_reference` 注入主任务；
   后者必须显式标记“未验证业务参考”，两者都不是执行证据。
4. `no_match`、身份不可用、超时或错误：不注入答案，主任务继续。
5. 只有用户的问题明确依赖内部含义而检索失败时，最终答复才说明“企业知识未验证”；
   不为每次后台检索展示工具过程。

首版普通飞书任务在 scope gate 直接得到 `not_required`，不调用 business provider。未来若
启用，只能使用当前请求用户自己的 UAT 并单独评审逐用户 OAuth；胡子豪固定 UAT 的后台
复用例外只存在于本机 RCA observer/provider，不提供给普通会话、其他工具、插件或模型。

## RCA 首轮增强

### 插入位置

目标设计不修改 host outbox dispatcher。未来独立 observer 完全运行在本机 Hermes host：
任务已经物化并封存 v2 request 后即可只读观察首轮输入；`preflight` 允许任务仍处于
`pending`/`claimed`/`running` 等进行态，不等待 VM 或报告完成，只有命中确定性业务信号
才执行首轮查询。`report_followup:1` 是独立阶段，只有原任务进入完成态且能通过封存
delivery manifest/contract 校验时才读取 sealed report，并在提取新注册术语后补查。业务
仓库、VM 代码和既有业务 schema 均不改动。

```text
durable admission
  -> claim
  -> official issue preread / enrich
  -> original v2 request / reservation / VM submit
  -> original VM core / report / required delivery

independent local Hermes host observer/provider
  -> observe materialized VM task read-only (preflight; task may still run)
  -> async preflight business lookup (best effort)
  -> observe completed sealed report read-only (follow-up only)
  -> deterministic business-term extraction
  -> async host supplemental lookup
  -> host-private owner-only reference + local audit receipt
```

Observer 复用 task-owned 定位符读取已经封存的 request，不向 control DB、outbox、VM
goal 或原 delivery store 写入。Aily create/poll 不占用 outbox lease，也不能打开原
dispatcher circuit。不能把网络调用放入 Kafka consumer、SQLite transaction、飞书
callback、VM、报告渲染或投递阶段。

目标 RCA 增强是独立 host-private 参考 lane，不改变当前一次性 VM core。**当前手动
shadow 原型没有 consumer seam，不能把计划或结果回灌给正在执行的 RCA，因此不能声称
它已经帮助 RCA。**未来要做到无感消费，必须单独评审并放开一个本机、只读、owner-only
consumer seam；首版仍不为 Aily 增加任何 VM hook/helper/artifact，也不扩展业务 schema。

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
把附录写回飞书；后续若需要通知，必须另行评审，不能复用 required delivery。首版附录
只写 active Hermes home 下的 host-private、task-bound 根，并由后续 Hermes 追问读取。

## 二阶段业务补查

首版 host-mediated 补查只在本机 Hermes host 上发生。observer 等原 RCA 已完成且报告
不可变封存后，只读报告并用版本化注册表确定性提取新出现的内部术语，再由同一 host
provider 补查并写 host-private owner-only addendum。原任务不等待、不 resume、不重投递。

```text
original RCA
  -> seal original report
  -> complete original task and required delivery without host wait

local Hermes host observer/provider
  -> detect completed sealed report
  -> validate report identity and seal read-only
  -> deterministically extract registered business terms
  -> perform create-once host lookup
  -> seal host-private owner-only business_knowledge_addendum_v1
```

约束：

- 首版最多一轮、最多两条 query；同一 `phase` 内相同 query digest 不重复调用。
- 输入只能是已完成、已封存、身份绑定一致的报告。observer 只读，不创建或修改 VM
  artifact、stage receipt、delivery contract、业务仓库代码或业务 schema。
- 术语提取只使用版本化注册表、token/词边界和固定 domain 规则，不调用第二个模型，也不
  把报告中的命令、链接或 prompt 指令转发给 provider。
- 最终 query 由 host 根据注册术语和业务 profile 重建；不发送原始帧、日志正文、附件、
  PDCL、用户信息、完整报告或报告中的自由文本指令。
- lookup job/receipt 无论成功或失败都是终态；只有 `grounded_match`/`answer_only` 生成
  host-private addendum，同一 hash 不允许形成查询循环。
- addendum 绑定同一 `submission_key`、generation、RCA contract、sealed report hash 和
  report seal receipt，但不 resume、不改写也不重新投递主 RCA。
- 补查失败时只记录“本轮业务增强不可用”，原 RCA 无需恢复，因为从未等待它。
- observer 读报告、术语提取、Keychain/lark-cli broker 或 addendum seal 任一步失败，均只
  终止本机增强并记录 host-local 状态，不改变原任务 disposition。
- 第二阶段默认本机 feature flag 关闭；完成幂等、report binding 和故障注入测试后再开启。

提取结果最多 `terms<=8`，只在 host 内存和 owner-only job state 中存在，不落入原报告或
任何业务 schema。首轮 `preflight` 只需验证 materialized task root、sealed request、身份和
文件绑定；不得把未完成任务当成报告输入。二阶段 observer 必须另外验证 task 已完成、report
为 regular file、无 symlink、大小有界，并核对 `submission_key`、generation、
`rca_contract_sha256`、报告 SHA-256 与既有 seal receipt；缺少现成 seal 或任一绑定不一致就
跳过补查。普通 receipt/context canonical JSON 使用机器契约中的 UTF-8、`ensure_ascii=false`、
sorted keys、compact separators、禁止 NaN；RCA execution-contract hash 使用上方单独声明的
`rca_contract_hash_json_v1`（`ensure_ascii=true`）。两种算法都固定版本，不要求业务仓库或
VM 新增 helper；不要求业务仓库或 VM 新增 helper。

Owner-only addendum 使用 exact schema：

```json
{
  "schema_version": "business_knowledge_addendum_v1",
  "mode": "active",
  "submission_key": "...",
  "generation": 1,
  "phase": "report_followup:1",
  "rca_contract_sha256": "...",
  "sealed_report_sha256": "...",
  "report_seal_receipt_sha256": "...",
  "term_registry_fingerprint": "...",
  "status": "answer_only",
  "influence": "reference_only",
  "query_hmac_sha256": "...",
  "answer_sha256": "656584fa205311b0846a70271f1778fce74d0990a9992967a1288040ec68c04c",
  "answer_bytes": 33,
  "provider_policy_fingerprint": "...",
  "identity_policy_fingerprint": "...",
  "lookup_receipt_relpath": "report-followup-1/<job_id>/lookup-receipt.json",
  "lookup_receipt_sha256": "...",
  "content": "owner-only bounded reference text"
}
```

Observer 对 `(submission_key,generation,phase,sealed_report_sha256)` 做原子 claim/CAS；
同一 key 只有一个 worker 可 create。每 generation 的 ledger 最多 `1` 条 preflight 和
`2` 条 report-followup query，create 总数不超过 `3`。重复观察、继续 poll 和读取已完成
context 不消耗新额度。

## Provider 与身份

交互式和后台 provider 必须分离：

| 场景 | 身份 | 边界 |
| --- | --- | --- |
| 普通飞书任务 | 当前请求用户的 UAT | 仅该用户会话；按用户权限过滤。 |
| 无人值守 RCA observer | 预登记的胡子豪固定 UAT | 仅本机 Hermes host observer/provider；不向任意会话开放。 |

首版 owner 决策是能力优先：后台 RCA 明确复用胡子豪固定 UAT。专用 service identity、
最小权限重构和身份借用风险专项后置；这项风险接受不等于取消以下首版硬边界：

- UAT 只由本机 Keychain/lark-cli broker 持有；provider 不接收明文 token 参数，不把 token
  写入 job、receipt、日志、业务仓库、业务 schema 或 VM。
- 每次 Aily create 前都必须通过指定 lark-cli `profile` 调 user-info，并将返回的 `open_id`
  和 `union_id` 与预登记值逐值精确比较；profile/open_id/union_id 任一不符都返回
  `identity_unavailable`，且不得发起 create。
- 该身份只绑定独立 RCA host observer/provider。普通会话、模型工具、插件和 VM 不能获取
  broker handle，也不能以“查业务知识”为由通用借权。
- profile、预登记身份摘要、auth mode 和批准的 Agent 策略共同形成
  `identity_policy_fingerprint`；明文 ID 和 token 不进入 host audit 正文或任何业务侧数据。

专用 Aily Agent 仍必须：

- 只启用企业知识检索；
- 关闭公开网络；
- 不绑定写型 MCP、飞书写工具或其他有副作用技能；
- 发布版本、知识范围和工具策略可形成 fingerprint；
- 调用应用和胡子豪用户在 Aily OpenAPI 用户范围内。

如果预登记身份精确校验、Keychain/lark-cli broker 或 UAT 生命周期任一不可用，provider
返回 `identity_unavailable`，RCA 继续原链；不能回退 tenant identity、Web、另一个员工
UAT 或另一个 Agent。

## 结果与持久化（真实 provider 目标合同）

未来真实 RCA provider job 必须使用 active governed Hermes home 的 host 私有根
`$HERMES_HOME/runtime/rca-prod/business-knowledge/<submission_key>/<generation>/`
持久化状态。POST 前先 durable seal `creating` job state；获得 terminal status 后、任何
消费者读取前，再封存 owner-only `business_knowledge_lookup_receipt_v1`。`phase_path` 只能是
`preflight` 或 `report-followup-<round>`，`job_id` 是机器契约中六元 create-once key 的 canonical
JSON SHA-256；因此最终 receipt 路径固定为
`<phase_path>/<job_id>/lookup-receipt.json`。目录为 `0700`、文件为 `0600`；完成 bounded
JSON、owner、regular-file、no-symlink、size 和 hash 校验后，使用 no-replace、同文件系统
rename 与父目录 fsync 原子 seal。所有状态和正文都留在本机 active Hermes home。

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

未来本机 coordinator 必须绑定 reviewed active home，不能接受每个 job 的 `HERMES_HOME` 或
状态根覆盖。所有 `*_relpath` 都以 generation 根为基准，规范化后不得绝对化、穿越父目录
或指向符号链接。

当前 Agent Chat 无稳定 source/activity/no-hit artifact，因此只能产生 `answer_only` 或失败
状态。只有 owner-only lookup receipt 通过 schema、条件字段、路径和 hash 校验后，消费者
才可生成 context/addendum；host-local safe receipt 只记录其 SHA-256，不记录私有路径或正文。

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

Host-private audit 层使用 exact `business_knowledge_safe_receipt_v1`，例如：

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

Safe receipt 顶层字段集合必须严格等于 `host_audit_receipt_common_fields_exact` 与对应
`host_audit_receipt_status_fields_exact[status]` 的并集；未知 key 立即拒绝。该 allowlist 是
主安全合同，`host_audit_receipt_forbidden_fields` 只是 defense-in-depth 检查。首版不把
该 receipt 投影到业务 schema、VM artifact 或 required delivery。

- `(submission_key, generation, phase, query_hmac_sha256,
  provider_policy_fingerprint, identity_policy_fingerprint)` 是唯一 create-once key。
  `phase` 只能是 `preflight`
  或有界的 `report_followup:<round>`；首轮和二阶段不会因文本相同而错误共用 job。
- create 已成功但 poll 结果不确定时不得自动创建第二个 chat。本机后台 provider 必须把
  create 与 poll 拆成两个可恢复操作，或在收到 chat ID 后先调用 durable callback 封存
  `chat_created` 再开始 poll；现有一次性 `run_agent_chat_user()` 不能原样承担该状态机。
- RCA create body 必须省略 `session_id`，每个六元 job 获得全新 Aily session。返回的
  session/chat ID 只可在该 owner-only job 内用于恢复 poll，绝不能用于另一个 job、task
  或 generation；现有 transport 的可选 `session_id` 参数在后台 provider 路径必须固定为
  `None`。
- owner-only context 权限为 `0600`，有正文大小和保留期上限。
- host-private audit receipt 只保留 exact allowlist 中的状态、长度、HMAC、策略版本、耗时和
  时间，不保存查询、
  答案、个人标识、chat/session ID 或凭据。
- 低熵缩写在 host-local audit receipt 中使用 keyed HMAC，避免裸 SHA 被字典反推。
- 不跨 generation 或授权主体复用答案；同 generation 仅用于幂等去重。
- `identity_policy_fingerprint` 绑定胡子豪预登记 profile/open_id/union_id 的摘要、auth mode
  和已批准知识范围，不含 token 或 open/union ID 明文；身份或 ACL 变化必须产生新 job，
  不能命中旧结果。
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
- lookup 不占用现有 dispatcher 长 lease；未来真实 provider 必须使用独立 durable
  enrichment state。当前手动原型没有耐久 create/poll 恢复能力。
- provider 返回文本按不可信输入处理，用 delimiter 隔离，不能执行其中的命令、链接、
  MCP 指令或“忽略之前规则”。
- business 与 local/live 冲突时保留 `knowledge_conflict`，以原始证据为准。
- Aily 超时、限流或停机不能改变原 execution request、canonical hash、VM submit、报告
  和投递结果；除新增可观察 receipt 外，禁用增强应能恢复原行为。

## 最小可逆原型

当前已存在的原型路径是：

- `~/.hermes/scripts/pnc_rca_aily_shadow_observer.py`
- `~/bin/pnc-rca-aily-shadow`

它是手动、一次性、未启用、未注册且非 daemon 的 planner。`inspect` 和
`run --provider dry-run` 都不发网络请求；无注册术语且未从 stdin 提供显式查询时返回
`not_triggered`（规范映射 `not_required`），不创建 provider job。真实 provider 当前
禁用且不可用：现有一次性 transport 不能耐久封存 create/chat ID 并恢复 poll，不能用
`--provider real` 宣称已经接入。

手动 dry-run 示例：

```bash
~/bin/pnc-rca-aily-shadow run \
  --latest-completed \
  --query-stdin \
  --provider dry-run \
  --pretty
```

命令启动后在终端输入业务问题，再按 `Ctrl-D` 结束 stdin。问题不写入命令参数或 shell
history。该命令只生成脱敏计划，不联网、不调用固定 UAT，也不回灌正在执行的 RCA。

未来无感能力需要单独启用本机 consumer seam，并在此之前实现可恢复的 create/poll、
真实 provider admission、owner-only 状态和故障测试。八步法只用于设计形成和评审，不是
原型或每个 RCA 的运行时状态机。

## 风险优先实施顺序

1. **手动 shadow 基线**：保持现有 `inspect`/`dry-run` 无网络，验证 deterministic trigger、
   `not_triggered`、stdin 和零业务副作用。
2. **可恢复 provider**：拆分 create/poll，耐久封存 chat ID；在此之前真实 provider 保持禁用。
3. **固定身份可用性**：预登记胡子豪 profile/open_id/union_id，并验证 Keychain/lark-cli
   broker；这不等于开启自动消费。
4. **Agent 策略与 receipt**：证明无公网、无写工具和来源边界；不能证明来源则保持
   `answer_only`。
5. **本机 consumer seam**：单独评审并显式放开后，才允许自动观察并将 owner-only
   reference 提供给后续 Hermes 消费；不得回写正在执行 RCA。
6. **身份风险专项后置**：另行评审专用 service identity、最小权限与审计迁移；不得反向
   阻塞已批准的首版固定 UAT 能力路径。

## 验收场景

| 场景 | 预期 |
| --- | --- |
| RCA 命中注册业务术语 | business required；不调用 Web fallback。 |
| RCA 无注册术语且无 stdin 显式查询 | `not_required` / 原型 `not_triggered`；不调用 Aily 或 Web，原 RCA 继续。 |
| 手动 `inspect` 或 `--provider dry-run` | 只输出脱敏计划；无网络、无 UAT、无 RCA 回灌。 |
| 手动原型请求真实 provider | 当前禁用/不可用；不能宣称上线。 |
| Aily timeout / 403 / no-match | 记录状态；VM、报告和投递保持原行为。 |
| 返回非空文本但无来源 | `answer_only/reference_only`，不能作为根因证据。 |
| 通用 Python 问题 | 不触发 business。 |
| 内部词和公开标准混合 | business 与 web 分开查询，不互相 fallback。 |
| 无人值守 RCA 使用胡子豪固定 UAT | 精确校验预登记 profile/open_id/union_id 后由 host provider 调用。 |
| 普通会话尝试借胡子豪 UAT | 不暴露 provider/broker；不能通用借权。 |
| 报告中新出现注册业务术语 | 未来 consumer seam 放开后，report seal 后确定性提词并最多一轮、两条 lookup；原任务不等待。 |
| business 与 live 证据冲突 | 标记冲突，以 live/原始证据为准。 |

相关设计材料与验收题目见[集成交接](feishu-aily-business-integration-handoff.md)；所有实现
与交接都必须以本文记录的最新 owner 决策和机器契约为准。
