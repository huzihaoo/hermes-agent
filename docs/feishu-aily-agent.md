# 飞书 Aily Agent + MCP 接入

你找到的地址是新版 Aily Agent：

```text
https://aily.feishu.cn/agents/agent_<...>
```

地址中的 `agent_<...>` 就是 **Agent ID**。它和调用方自建应用的
`cli_<...>` App ID 是两个不同的值，也不需要旧知识库 API 使用的
`spring_<...>` App ID。

## 这条链路如何执行 MCP

MCP 服务是在 Aily Agent 后台配置的服务端能力。调用方只需要向 Agent
发起对话；请求体不传 MCP URL、工具名或 secret，Agent 会按照它自己的
配置决定是否调用 MCP，并把最终文本/产物返回。也就是说，Hermes 的
工具是 Agent Chat 的调用方，不是 MCP 客户端。

注意 Agent 的对话模式会影响路由：纯“知识问答”模式只执行知识问答，
不会主动调用 MCP；要验证 MCP，应使用“模型推理”或“混合调度”模式，
并确保对应技能/MCP 已提交、发布且问题能明确命中它。

## 先做飞书侧配置

1. 在飞书开放平台撤销并轮换曾经粘贴到聊天或 session 的 App Secret。旧值视为已暴露，不能再使用。
2. 在调用方自建应用的「权限管理」中开通 `aily:agent_chat:write` 和 `aily:agent_chat:read`。Hermes 默认采用异步创建后轮询，以使用公开定义的 JSON 结果契约。
3. 在 Aily 智能体后台确认该 Agent 已发布，并确认 Agent 中的 MCP 服务已经发布且凭据有效。
   在「使用渠道 → Open API」中不只是打开渠道开关，还要：
   - 在“支持调用的身份类型”中勾选“应用身份”；
   - 将调用方自建应用的 `cli_...` App ID 加入“应用身份 · 可调用应用范围”。该列表为空表示不允许其他应用调用；
   - 确认调用方应用与 Aily Agent 在同一租户。当前 OpenAPI 不支持跨租户调用。
   OpenAPI 渠道配置变更立即生效，不要求因此重新发布 Agent；Agent 内容和 MCP 技能本身仍应处于已发布状态。
4. 本 Hermes 工具使用 `tenant_access_token`，因此必须完成上一步的“应用身份”与可调用应用范围配置。官方接口也支持 `user_access_token`；本工具为避免把用户 OAuth 引入普通群聊，当前只使用 tenant 身份。
   TAT 只代表调用方应用，不等于终端用户委托。下游技能/MCP 使用什么身份、能访问哪些资源，由每个技能/MCP 的配置决定，接入时必须单独核验；需要代表用户访问资源时，不能假定 TAT 会继承用户权限。

## Hermes 配置

在受管的 0600 环境中配置以下变量，不要把 secret 放在命令行：

```text
FEISHU_AILY_AUTH_APP_ID=cli_<调用方自建应用 App ID>
FEISHU_AILY_AUTH_APP_SECRET=<轮换后的 secret>
FEISHU_AILY_AGENT_ID=agent_<Aily 智能体详情页地址栏中的 ID>
FEISHU_AILY_DOMAIN=feishu
```

启用独立的 Agent toolset：

```text
/tools enable feishu_aily_agent
```

工具名为 `feishu_aily_agent_chat`，输入 `content`，可选 `session_id` 和
已上传的 `agent_attachment_ids`。旧的 `feishu_aily` toolset 仍保留给
`spring_...` 数据知识问答，不要把两种 ID 互换。

## API 形状

```text
POST https://open.feishu.cn/open-apis/aily/v1/agents/:agent_id/chats
```

本文继续使用当前飞书开放平台接口详情中的 `/chats` 路径。Aily 功能手册的
新版资源表同时展示了 `/agent_chats` 命名，但它链接的接口详情和官方 SDK
仍使用 `/chats`；本实现以具体接口详情/API 控制台为准，不自行替换路径。

Hermes 创建对话时发送的最小请求是：

```json
{
  "user_message": {
    "content": [{"type": "text", "text": "OOI是什么?"}]
  },
  "stream": false
}
```

文本最多 10000 字符，附件最多 8 个。Hermes 只在轮询结果出现明确成功
status 后返回答案；实现随后轮询
`GET /open-apis/aily/v1/agents/:agent_id/chats/:agent_chat_id`，不会把
processing 的半成品当作最终答案。结果会保留有限的
`agent_artifact_id`/`artifact_type` 元数据，不会把完整 MCP 原始响应回传。

Aily 功能手册把 `Queued` 视为处理中、`Completed`/`Failed` 视为终态；实现
只把 `Completed` 作为成功，不会把带有中间文本的 `Queued` 提升为答案。
其链接的 GET 接口详情虽然给出 `status=Cancelled`、`finish_reason=stop` 且带
正文的孤立样例，但与新版状态说明冲突；在没有目标租户实测 receipt 前，
`Cancelled` 一律失败关闭，避免把部分答案当作最终知识答案。

## 脱敏 smoke

默认只检查配置，不发网络请求：

```bash
python scripts/feishu_aily_agent_smoke.py --pretty
```

确认权限和 Agent 发布状态后，再显式执行一次受控请求。敏感问题用 stdin，
避免进入 shell history 或进程列表：

```bash
printf '%s' 'OOI是什么?' | \
  python scripts/feishu_aily_agent_smoke.py \
    --execute --question-stdin --pretty
```

默认 smoke 只输出 HTTP 状态、响应类型、完成状态、答案是否存在/长度和产物
数量，不输出答案正文、token、secret、完整 SSE 或 MCP 原始数据。人工需要核对
答案时可显式增加 `--show-answer`，最多输出 300 字符；不要把该模式用于共享日志。
它不是 resident gateway
的发布门禁；真正上线前仍需用 active secret scope 做 canary，再按 Hermes
release governance 物化和重启。

人工核对本次答案：

```bash
printf '%s' 'OOI是什么?' | \
  python scripts/feishu_aily_agent_smoke.py \
    --execute --question-stdin --show-answer --pretty
```

常见渠道/身份错误：

| code | 处理 |
| --- | --- |
| `10001` / `2700001` | 请求参数错误，检查 Agent ID、问题和请求体。 |
| `10002` | `agent_chat_id` 不存在或不属于当前调用身份。 |
| `10006` | OpenAPI 渠道未开启。 |
| `10007` | 检查身份类型、用户/应用可用范围、同租户要求及附件/产物归属。 |
| `10008` | 当前租户尚未开通 Aily OpenAPI，需要租户管理员处理。 |
| `10009` | OpenAPI 渠道未勾选当前身份；本工具需要“应用身份”。 |
| `10010` | 调用方 `cli_...` App ID 未加入“可调用应用范围”。 |
| `10011` | 用户身份不在可用范围；当前 tenant-only 工具通常不会走此分支。 |
| `50001` | Aily 内部错误；做一次有界重试，持续出现时携带 log id 反馈。 |

`10009`～`10011` 来自该帮助页链接的开放平台接口概述；帮助页当前错误码表中
部分身份错误仍标注“待分配”。排障时始终以实际 HTTP、`code`、`msg` 和
脱敏后的 log id 为准，不依赖本地映射替代服务端结果。

## lark-cli 边界

`lark-cli api` 可以发送这个 raw endpoint，但稳定完成一次对话需要先发
非流式创建请求，再调用 GET 结果接口；通用 CLI 不会替你做轮询。稳定调用
请使用 Hermes 工具或本目录的 Python smoke。若自行使用 `stream=true`，
官方只承诺返回 SSE，未公开固定的事件字段，不能假定 stock CLI 能解析。

只做创建请求时，CLI 形状如下（不要把 secret 放进命令行）：

```bash
lark-cli api POST \
  "/open-apis/aily/v1/agents/${FEISHU_AILY_AGENT_ID}/chats" \
  --as bot \
  --data '{"user_message":{"content":[{"type":"text","text":"OOI是什么?"}]},"stream":false}'
```

读取返回的 `agent_chat_id` 后，再调用对应 GET，并确认终态
`status=Completed`；`finish_reason` 只作诊断，不要把创建响应本身当作最终答案。

## 官方参考

- [发起智能体对话](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/create)
- [获取对话结果](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/get)
- [选择和获取访问凭证](https://open.feishu.cn/document/uAjLw4CM/ugTN1YjL4UTN24CO1UjN/trouble-shooting/how-to-choose-which-type-of-token-to-use)
- [飞书 Aily 能力模式与技能调度](https://www.feishu.cn/content/kdvbmrpn)
- [通过 Open API 调用智能体](https://aily.feishu.cn/hc/1u7kleqg/t973w36g)
