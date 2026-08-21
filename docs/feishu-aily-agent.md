# 飞书 Aily Agent 企业知识问答

相关文档：

- [候选测试报告](feishu-aily-agent-test-report.md)
- [Hermes 统一知识检索路由](hermes-knowledge-retrieval-routing.md)

当前 Aily 工具是独立、默认关闭的候选。用户身份 API canary 已打通，但生产 gateway
尚未启用。目标体验不是让用户手工说“查企业知识库”，而是由统一路由根据关键词和
任务上下文，在 Web、本地知识工程和 Aily 企业知识之间自动选择。RCA 属于企业知识
强制检索任务；该自动路由仍待 RCA 正式生产分支实现。

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

本机企业知识问答采用固定用户模式：`lark-cli` 只作为 OAuth token broker，
Hermes 不读取、不落盘或打印 UAT。每次请求先以 `--as user` 调用
`/open-apis/authen/v1/user_info`，同时核对调用应用内的 `open_id` 和跨应用稳定的
`union_id`。只有来自飞书、且当前会话 `union_id` 精确匹配该固定用户时才可调用；
CLI、cron、API、其他飞书用户和缺失身份的上下文全部失败关闭，且不会回退 TAT。

## 飞书侧配置

1. 调用方自建应用开通 `aily:agent_chat:write` 和
   `aily:agent_chat:read`，并完成发布。
2. Aily 智能体「使用渠道 -> Open API」开启用户身份，并把目标员工加入用户
   可用范围。调用应用和 Aily Agent 必须在同一租户。
3. 企业知识管理员在 `https://aily.feishu.cn/ai/play-management/tool-config`
   开启企业知识，并选择“全部”或明确的目标知识空间。
4. Agent 开启知识空间检索并挂载目标知识。直连知识仍按对话用户权限过滤，
   不需要给调用应用额外增加 `wiki`/`docx` scope。
5. 该 Agent 必须关闭公开网络兜底，或使用只执行知识检索的知识问答策略。
   未命中企业知识时应返回“未检索到”，不能改用互联网搜索或通用知识猜测。

MCP 是 Agent 后台配置的服务端能力，请求体不传 MCP URL、工具名或 secret。
是否执行 MCP 取决于 Agent 的对话模式和已发布技能。下游 MCP 使用什么身份、
访问什么数据必须单独核验；外层 UAT 不自动等价于 MCP 的用户委托。

## Hermes 用户模式配置

UAT 保存在隔离的 `lark-cli` 凭据目录中。目录必须归当前用户所有且权限为
`0700`；配置文件保持 `0600`。以下值放入受管 Hermes 配置，绝不写入仓库：

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

`FEISHU_AILY_AUTH_MODE` 必须显式配置；缺失时工具失败关闭，不会静默回退到
tenant 身份。用户模式固定给每个问题增加“只允许企业知识、禁止公网、未命中即
停止”的约束，不提供关闭开关。它只是客户端纵深防护，不是 grounded 证明；
Aily 后台的工具和兜底策略仍是权威控制点。

仅在开发 home 或受管隔离 staging 中启用独立 toolset；不要对 active home
直接执行该命令：

```bash
hermes tools enable feishu_aily_agent --platform feishu
```

该 toolset 只允许飞书平台使用。工具名为 `feishu_aily_agent_chat`，输入
`content`，可选 `session_id` 和已上传的 `agent_attachment_ids`。

若另有明确的应用身份场景，可设置 `FEISHU_AILY_AUTH_MODE=tenant`，并配置
`FEISHU_AILY_AUTH_APP_SECRET`。tenant 模式与本企业知识用户模式相互独立。

## 任务内使用

交互式任务中，主代理传入业务问题，用户不需要提供 Agent ID 或身份字段：

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
`grounded=true`。只在同一用户、同一任务需要连续追问时复用 `session_id`；
不得跨用户或跨任务共享会话。`no_match`、身份或协议错误不能自动降级到
Web 搜索内部含义。

当前候选允许主代理显式选择此工具。未来统一路由上线后，业务关键词和
RCA 任务上下文将自动触发；具体契约见
[Hermes 统一知识检索路由](hermes-knowledge-retrieval-routing.md)。

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
禁止把该输出写入共享日志或公开产物。smoke 只验证凭据、endpoint 和知识回答，
不等价于 resident gateway 发布门禁。

## 发布与生效

`gateway/*.py` 和 `tools/*.py` 需要重启常驻 gateway 才能生效；当前 gateway 会在
每轮重新读取 `config.yaml`，并重新加载当前 profile 的凭据。因此 active home 的
配置修改可能先于代码切换被旧 resident 观察到。正式发布必须遵循
`HERMES_RUNTIME_GOVERNANCE_RUNBOOK.md` 和 `pnc-business-prod-effect-chain.md`，禁止
在代码物化和发布门之前对 active home 运行 `tools enable`：

1. 在候选工作树完成受影响测试并形成一个 clean commit。
2. 在 owner-only staging home 中准备目标 `config.yaml`/`.env`，两个文件路径都
   必须为绝对路径。启用时清空继承环境，同时钉住 staging home、config
   和 env，并调用候选工作树的 CLI，不能用全局 `hermes`：

   ```bash
   env -i \
     HOME=/Users/songying USER=songying LOGNAME=songying \
     PATH=/usr/local/bin:/usr/bin:/bin TMPDIR=/private/tmp LANG=zh_CN.UTF-8 \
     HERMES_HOME=/Users/songying/.hermes-release-staging/feishu-aily-agent \
     HERMES_CONFIG_PATH=/Users/songying/.hermes-release-staging/feishu-aily-agent/config.yaml \
     HERMES_ENV_PATH=/Users/songying/.hermes-release-staging/feishu-aily-agent/.env \
     /usr/local/bin/uv --directory \
       /Users/songying/.codex-worktrees/feishu-aily-knowledge-qa-20260813 \
       run --frozen hermes tools enable feishu_aily_agent --platform feishu
   ```

   命令前先以 owner-only 权限创建 staging 目录，并用受管流程写入目标
   `.env`；不得从 active home 复制未经审查的其他凭据。然后回读三个路径
   确认 CLI 只修改了 staging，计算目标 config/env SHA。预置凭据时
   active toolset 必须继续关闭。
3. 根据 owner 的明确发布意图，用受管 materializer 物化精确 commit；禁止直接
   修改 active detached runtime。对物化代码、staging 配置和目标 manifest 运行
   governance、strict drift、release fingerprint 及发布门。
4. 在同一个受管 promotion transaction 中原子切换 active config/env、更新
   `LIVE_MANIFEST.json` 绑定并重载 resident，不能把这些步骤拆成提前生效的手工修改。
   若 LaunchAgent 的
   program/working directory 未变化，只重启 `ai.hermes.gateway`；若路径变化，按
   runbook 执行 `bootout/bootstrap`，不能只 `kickstart`。
5. 核对新 PID、运行目录/commit、公开 health、`gateway_state.json` PID，以及
   Feishu platform connected；回读 active config 的 raw SHA/semantic SHA 和 env 的
   byte SHA，确认它们与 manifest 绑定一致。
6. 由被固定授权的员工在真实飞书会话提问内部 canary，确认 session 只调用
   `feishu_aily_agent_chat`。同时保留 Aily 已发布版本/后台策略或活动日志的证据，
   证明知识未命中时不会走公网兜底；仅凭答案正文不能证明 grounded。默认回执
   不得记录内部答案正文。

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
| `50001` | Aily 内部错误；有界重试一次，持续出现时携带脱敏 log id 排查。 |

## 官方参考

- [发起智能体对话](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/create)
- [获取对话结果](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/get)
- [通过 Open API 调用智能体](https://aily.feishu.cn/hc/1u7kleqg/t973w36g)
- [企业知识检索范围](https://aily.feishu.cn/hc/1u7kleqg/2csyds1b)
- [知识连接方式与权限](https://aily.feishu.cn/hc/1u7kleqg/knowledge_connection_mode)
