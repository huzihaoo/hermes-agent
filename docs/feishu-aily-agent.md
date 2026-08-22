# 飞书 Aily Agent 企业知识问答

相关文档：

- [候选测试报告](feishu-aily-agent-test-report.md)
- [Hermes 业务知识检索接入设计](hermes-knowledge-retrieval-routing.md)
- [业务接入设计工作记录](feishu-aily-business-integration-worklog.md)
- [本机 RCA observer 交接](feishu-aily-business-integration-handoff.md)

当前 Aily transport/tool 是独立、默认关闭的候选。固定员工用户身份 API canary 已
打通；2026-08-21 还在无飞书 session context 的本机独立进程中通过 user smoke 完成了
一次安全调用。本机手动 shadow planner 原型文件已存在，但自动 RCA observer/consumer
尚未实现或启用，production gateway 也未启用该工具。

Owner 对运行时的最新决定仍是把首版范围收敛为**仅本机 Hermes host 的 RCA observer**：可复用胡子豪
固定 UAT，能力优先；权限代理、审计归属、ACL 漂移和 token 生命周期风险登记为
`accepted/deferred`，不作为首版门禁。运行时凭据/provider/consumer seam 不得进入
PNC/RCA 业务链、VM、业务 schema/产物，也不得成为普通 Hermes/飞书对话或其他调用方
可借用的通用工具。Aily 回答
只作 owner-only reference，失败继续原 RCA，不能当成执行证据。

2026-08-22 owner 另行允许 exact candidate 源码合入当前正式 Host。源码发布与
运行时启用是两个门：合入后 Aily toolsets/provider/consumer seam 继续 default-off，
不传递凭据、不做真实飞书调用、不回写 RCA，也不因合入自动重启 gateway。

这项决定不等于自动 observer 已实现或已发布。本文件中现有 tool/smoke 和手动 shadow
planner 只提供 API、身份、离线规划和隔离参照；当前 planner 未启用、未注册、非 daemon，
没有 consumer seam，不能把结果回灌给正在执行的 RCA。候选源码是否已合入
不改变这一运行时判定。

新版 Aily 智能体详情页地址中的 `agent_<...>` 是 Agent ID：

```text
https://aily.feishu.cn/agents/agent_<...>
```

它和调用方自建应用的 `cli_<...>` App ID 是两个值，也不是旧数据知识
问答接口使用的 `spring_<...>` App ID。

## 身份与知识边界

Aily Agent Chat API 同时支持应用身份和用户身份：

- `tenant_access_token` 只代表调用方应用，不继承某个员工的企业知识权限。
- `user_access_token` 代表完成 OAuth 的员工。直连企业知识按该用户的可搜
  权限过滤。

候选交互路径采用固定用户模式：`lark-cli` 只作为 OAuth token broker，Hermes 不读取、
不落盘或打印 UAT。每次请求先以 `--as user` 调用
`/open-apis/authen/v1/user_info`，同时核对调用应用内的 `open_id` 和跨应用稳定的
`union_id`。候选工具还要求当前飞书会话匹配该用户；这项历史实现已用于 canary，
但不会作为首版后台入口。独立 user smoke 可以在没有飞书 session context 时直接验证
固定 profile 和用户身份，这条路径只用于验收/诊断；它已证明 standalone transport
能力，不代表通用 CLI/API 被授权借权。

首版后台 observer 没有可继承的飞书会话身份，因此必须由**隔离的 observer 专用
broker**在每次 create 前核对 token 实际用户与配置中的胡子豪 `open_id + union_id`。
任一字段缺失或不匹配都在发问题前失败，且不回退 TAT。该 broker 只能服务本机 RCA
observer；普通对话、其他用户、CLI/API、resident 业务 worker、Kafka/outbox 和 VM 都
不能借用。owner-only 人工 smoke 是唯一允许的非运行时使用，仅用于验收和诊断。

首版接受并延后的风险包括：查询权限按胡子豪权限过滤、飞书侧审计主体显示胡子豪、
权限可能漂移，以及 OAuth 到期/撤权可能导致增强不可用。这些风险未被消除，但不阻断
首版；身份错配、token 泄露、扩大调用面和把答案写给更广受众仍不在风险接受范围内。

## 飞书侧配置

1. 调用方自建应用开通 `aily:agent_chat:write` 和
   `aily:agent_chat:read`，并完成发布。
2. Aily 智能体「使用渠道 -> Open API」开启用户身份，并把目标员工加入用户
   可用范围。调用应用和 Aily Agent 必须在同一租户。
3. 企业知识管理员在 `https://aily.feishu.cn/ai/play-management/tool-config`
   开启企业知识，并选择“全部”或明确的目标知识空间。
4. Agent 开启知识空间检索并挂载目标知识。直连知识仍按对话用户权限过滤，
   不需要给调用应用额外增加 `wiki`/`docx` scope。
5. 上线前门禁必须确认该 Agent 已关闭公开网络兜底，或使用只执行知识检索的知识
   问答策略；当前本机没有后台配置/活动日志证明这一点。未命中企业知识时应返回
   “未检索到”，不能改用互联网搜索或通用知识猜测。

MCP 是 Agent 后台配置的服务端能力，请求体不传 MCP URL、工具名或 secret。
是否执行 MCP 取决于 Agent 的对话模式和已发布技能。下游 MCP 使用什么身份、
访问什么数据必须单独核验；外层 UAT 不自动等价于 MCP 的用户委托。

## 本机固定用户配置

UAT 保存在隔离的 `lark-cli` 凭据目录中。目录必须归当前用户所有且权限为
`0700`；配置文件保持 `0600`。以下值只放入 owner-only 本机 Hermes 配置，绝不写入
仓库、业务 schema、RCA artifact、VM 输入或共享 receipt：

```text
FEISHU_AILY_AUTH_MODE=user
FEISHU_AILY_AUTH_APP_ID=cli_<调用方自建应用 App ID>
FEISHU_AILY_AGENT_ID=agent_<Aily 智能体 ID>
FEISHU_AILY_USER_LARK_CONFIG_DIR=/absolute/0700/lark-cli-config
FEISHU_AILY_USER_OPEN_ID=ou_<该调用应用下的用户 open_id>
FEISHU_AILY_USER_UNION_ID=on_<跨应用稳定 union_id>
```

首次接入时，以调用方 App ID 同名的单用户 profile 建立隔离凭据目录。下列命令
会从 stdin 读取 App Secret，不要把 secret 写进参数；随后由目标员工本人完成
device OAuth：

```bash
export LARKSUITE_CLI_CONFIG_DIR=/absolute/0700/lark-cli-config
export CALLER_APP_ID=cli_your_app_id
install -d -m 700 "$LARKSUITE_CLI_CONFIG_DIR"
lark-cli profile add \
  --name "$CALLER_APP_ID" \
  --app-id "$CALLER_APP_ID" \
  --app-secret-stdin
lark-cli --profile "$CALLER_APP_ID" auth login \
  --scope 'aily:agent_chat:write aily:agent_chat:read auth:user.id:read offline_access'
lark-cli --profile "$CALLER_APP_ID" api GET \
  /open-apis/authen/v1/user_info --as user --format json
```

最后一条命令必须返回预期员工；将该调用应用下的 `open_id` 和跨应用稳定的
`union_id` 登记到受管配置。姓名只用于人工核对，不能作为授权键。不要切换全局
active profile，也不要把 user-info 输出放进共享日志。

`FEISHU_AILY_AUTH_MODE` 必须显式配置；缺失时 broker 失败关闭，不会静默回退到
tenant 身份。用户模式固定给每个问题增加“只允许企业知识、禁止公网、未命中即
停止”的约束，不提供关闭开关。它只是客户端纵深防护，不是 grounded 证明；Aily
后台的工具和兜底策略仍是权威控制点。

首版 RCA observer **不得**在 active gateway 或普通对话 profile 中执行
`hermes tools enable feishu_aily_agent`。候选工具名 `feishu_aily_agent_chat` 只保留作
实现和测试参照，不是首版运行时暴露面。observer 必须通过自己的本机 broker API 调用，
且该 API 不注册到通用模型 tool schema。

tenant 模式不属于首版 RCA observer，不能在 user identity 失败时作为 fallback。

## 本机 RCA observer 使用边界

observer 构造的业务问题不包含 Agent ID 或身份字段；以下只展示有界问题内容的形状，
不是允许普通对话直接调用固定 UAT 的接口：

```json
{
  "content": "OOI在ACC/AEB业务中的定义和正常切换规则是什么？"
}
```

成功结果的核心字段为：

```json
{
  "success": true,
  "content": "...",
  "answer_available": true,
  "status": "Completed",
  "session_id": "...",
  "agent_chat_id": "..."
}
```

`answer_available=true` 只表示返回了文本，不等于企业知识已命中或
`grounded=true`。每个 observer job 使用全新 session，不跨 task/generation 共享对话
历史。`no_match`、身份或协议错误不能自动降级到 Web 搜索内部含义。

首版目标有两个本机、异步且不影响原链的机会点；当前仅离线 shadow planner 可检查计划：

1. 命中确定性注册术语或 stdin 显式查询时，从既有只读 RCA locator/允许字段构造至多
   一条 required query；仅 `task_type=rca` 或无信号时为 `not_required`（手动 planner
   显示 `not_triggered`），不调用 Aily 或 Web；
2. 原报告完成后，未来 observer 只有在 sealed delivery manifest/contract 存在且身份/hash
   校验通过时，才用版本化确定性规则从允许字段提取、排序、去重和截断新注册术语；
   缺失或不匹配时 fail closed，仅记本机状态。命中时执行至多一轮、两条补查 query。

第二阶段不要求 VM 生成 gap，不增加业务 schema 或 artifact，也不恢复、重跑或重新投递
主任务。报告内容按不可信数据处理；完整 issue/report、ID、URL、PDCL、帧、日志、用户、
评论、附件和既有 root cause 都不能进入 query。

RCA 中的 Aily 内容只用于解释术语、提示待验证假设和聚焦人工后续分析。结果只写
owner-only 本机 reference/receipt，不回写原报告、事项或投递。查询超时、未命中、身份
不可用或仅返回无来源文本时，原 VM、报告和投递链保持不变，也不降低或提高现有证据
结论。

## API 形状

创建对话：

```text
POST /open-apis/aily/v1/agents/:agent_id/chats
```

请求使用非流式 JSON：

```json
{
  "user_message": {
    "content": [{"type": "text", "text": "OOI是什么?"}]
  },
  "stream": false
}
```

创建接口只返回 `agent_chat_id`/`session_id`。随后轮询：

```text
GET /open-apis/aily/v1/agents/:agent_id/chats/:agent_chat_id
```

只有精确 `status=Completed` 才返回答案；`Queued`/`Running` 继续等待，
`Failed`/`Cancelled` 失败关闭。相邻、完全相同的顶层文本项会折叠一次，合法的
非相邻重复内容保留。

Aily 功能手册的新资源表曾展示 `/agent_chats`，但当前接口详情和官方 SDK 仍
使用 `/chats`。本实现锁定当前具体接口详情，不做可能重复创建会话的自动 fallback。

## 本机 shadow planner 示例

手动原型通过 stdin 接收问题，命令行不包含业务问题；`dry-run` 只输出脱敏计划，不联网、
不调用 UAT，也不回灌正在执行的 RCA：

```bash
~/bin/pnc-rca-aily-shadow run \
  --latest-completed \
  --query-stdin \
  --provider dry-run \
  --pretty
```

启动后在终端输入问题，再按 `Ctrl-D` 结束 stdin。无注册术语且没有显式输入时，计划返回
`not_triggered`（规范状态 `not_required`）。report-followup 没有 sealed manifest/contract
或绑定校验失败时也 fail closed，不补查。真实 provider 当前禁用/不可用，没有耐久
create/poll 恢复；该原型未启用、未注册、非 daemon。`--latest-completed` 目前只定位一个
已物化任务，不是 completed sealed report locator；报告二阶段尚未接入数据库封存联结。

## 用户身份 smoke

问题必须从 stdin 输入，避免进入 shell history 和进程列表。默认 receipt 只包含
完成状态、轮询次数、答案长度和产物数量，不包含答案正文或用户标识：

```bash
python scripts/feishu_aily_agent_user_smoke.py \
  --env-file /Users/songying/.hermes/.env \
  --question-stdin --pretty
```

命令启动后直接在终端输入问题，再按 `Ctrl-D` 结束 stdin。不要把内部问题写进
命令参数、shell 脚本或共享日志。脚本将 `.env` 作为数据解析，不会 `source` 或
执行其内容；只读取用户 smoke 所需的非 secret 白名单字段，忽略 App Secret、
UAT 和其他环境项。环境文件必须是当前用户持有的绝对路径、普通非符号链接文件，
且权限严格为 `0600`。显式配置参数可覆盖白名单值，省略 `--profile` 时默认使用
调用方 App ID。

本机人工验收时可显式增加 `--show-answer`，最多显示 300 字；答案是内部知识，
禁止把该输出写入共享日志、业务仓或 RCA 产物。smoke 只验证凭据、endpoint 和知识
回答，不证明后台 observer 已实现或已启用。

## 本机 observer 生效边界

本文件记录的 observer 验收没有生产生效动作。即使 default-off 源码合入正式 Host，
当前 production gateway toolset 仍保持关闭，不因源码合入执行 gateway restart、VM 下发、
凭据配置或业务 schema migration。

后续实现本机 observer 时，至少满足以下条件才能在本机显式启用：

1. 能力代码、配置、凭据引用和新增状态都位于 owner-only Hermes 本机边界，业务仓和
   RCA artifact root 的 before/after diff 为零。
2. observer 专用 broker 每次 create 前精确核验胡子豪 `open_id + union_id`，不把 UAT
   或 App Secret 暴露给 observer 进程、日志和 receipt。
3. 通用 Hermes/飞书对话、其他用户、CLI/API、Kafka/outbox、dispatcher 和 VM 都没有
   broker 调用入口；候选 toolset 保持关闭。
4. 第一阶段的 deterministic trigger 与 completed-report 确定性 extractor 通过
   allowlist、命中后 required attempt、无信号 `not_triggered`、幂等、注册术语、固定排序/
   上限和 prompt-like 文本负例测试；二阶段最多一轮两条 query。
5. timeout、403、429、5xx、no-match、bad JSON、oversize、crash、create-unknown 和本地
   存储失败都只终止 observer job，不改变原 RCA、报告或投递。
6. owner-only 人工通过 stdin 重新执行内部 canary 以证明真实能力，默认 receipt 不记录问题、
   答案正文、身份或 token；缺少 provenance 时只标 `answer_only`。

这些是技术必要条件，不是生产授权。实际启用还必须有独立、新鲜的批准，
精确绑定 Host commit/tree、运行时配置、凭据作用域、影响的 resident 和允许 effects。

专用服务身份、细粒度 ACL、审计触发者映射和 token 生命周期自动化已由 owner 延后，
不属于上述首版门禁。若未来扩大到多用户、通用对话、远端服务、业务仓或 VM，必须重新
评审，不能沿用本次风险接受。

## lark-cli 边界

所有 API 子命令都固定 profile 并显式使用 `--as user`；请求 JSON 通过
`--data -` 从 stdin 传入。通用 `lark-cli api` 不会自动完成 create -> poll，
因此由本工具和 smoke 负责有界轮询、输出上限、总超时和进程组清理。

## 常见错误

| code | 处理 |
| --- | --- |
| `10001` / `2700001` | 检查 Agent ID、问题和请求体。 |
| `10002` | chat 不存在，或 create/poll 使用了不同身份。 |
| `10006` | Open API 渠道未开启。 |
| `10007` | 检查身份类型、用户/应用可用范围、同租户和附件归属。 |
| `10008` | 租户尚未开通 Aily OpenAPI。 |
| `10009`～`10011` | 兼容性提示；检查身份开关和用户/应用范围，以实际 `code/msg/log_id` 为准。 |
| `50001` | 当前 observer 不调用真实 provider；未来只有在请求尚未发出或已有 durable idempotency 的 broker 中，才可按独立策略有界重试，并携带脱敏 log id 排查。 |

## 官方参考

- [发起智能体对话](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/create)
- [获取对话结果](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/get)
- [通过 Open API 调用智能体](https://aily.feishu.cn/hc/1u7kleqg/t973w36g)
- [企业知识检索范围](https://aily.feishu.cn/hc/1u7kleqg/2csyds1b)
- [知识连接方式与权限](https://aily.feishu.cn/hc/1u7kleqg/knowledge_connection_mode)
