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
3. 在 Aily 智能体后台确认该 Agent 已发布、调用方应用/应用身份有访问权限，并确认 Agent 中的 MCP 服务已经发布且凭据有效。
4. 如果使用 `tenant_access_token`，在 Aily 后台开启“支持使用应用身份调用 API 和 SDK”。官方接口也支持 `user_access_token`；本 Hermes 工具为避免把用户 OAuth 引入普通群聊，当前只使用 tenant 身份。
   使用 tenant 身份时，Agent 技能/MCP 通常按 Aily 配置的 API/匿名用户权限执行；如果某个 MCP 必须代表具体终端用户访问资源，应改用经过授权的 user token，并在 Aily 侧给该身份配置权限。

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

Hermes 创建对话时发送的最小请求是：

```json
{
  "user_message": {
    "content": [{"type": "text", "text": "OOI是什么?"}]
  },
  "stream": false
}
```

文本最多 10000 字符，附件最多 8 个。Hermes 只在轮询结果出现明确完成
状态（`finish_reason` 或明确终态 status）后返回答案；实现随后轮询
`GET /open-apis/aily/v1/agents/:agent_id/chats/:agent_chat_id`，不会把
processing 的半成品当作最终答案。结果会保留有限的
`agent_artifact_id`/`artifact_type` 元数据，不会把完整 MCP 原始响应回传。

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

smoke 只输出 HTTP 状态、响应类型、完成状态、答案截断片段和产物数量，
不输出 token、secret、完整 SSE 或 MCP 原始数据。它不是 resident gateway
的发布门禁；真正上线前仍需用 active secret scope 做 canary，再按 Hermes
release governance 物化和重启。

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

读取返回的 `agent_chat_id` 后，再调用对应 GET，并确认 `finish_reason`/终态
`status`，不要把创建响应本身当作最终答案。

## 官方参考

- [发起智能体对话](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/create)
- [获取对话结果](https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/aily-v1/agent-agent_chat/get)
- [选择和获取访问凭证](https://open.feishu.cn/document/uAjLw4CM/ugTN1YjL4UTN24CO1UjN/trouble-shooting/how-to-choose-which-type-of-token-to-use)
- [飞书 Aily 能力模式与技能调度](https://www.feishu.cn/content/kdvbmrpn)
