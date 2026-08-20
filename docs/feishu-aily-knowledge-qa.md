# 飞书 Aily 知识问答接入

> 如果你的 Aily 地址是 `aily.feishu.cn/agents/agent_...`，请阅读
> [Aily Agent + MCP 接入](feishu-aily-agent.md)。本页只对应旧的
> Data Knowledge API（`spring_...` app ID）。

这份说明对应 Hermes 的可选 `feishu_aily` toolset。当前实现位于候选工作树，默认关闭；没有把它绑定到 active release、LaunchAgent 或现有 `.env`。

## 两个 App ID

请求涉及两个职责不同的 App ID。本地 smoke 会保守拒绝两者取值相同，以尽早发现把调用方 ID 填进目标路径的常见误配；这是本地 guard，不是官方 schema 约束。

| 配置 | 用途 |
| --- | --- |
| `FEISHU_AILY_AUTH_APP_ID` + `FEISHU_AILY_AUTH_APP_SECRET` | 调用方自建飞书应用，用于换取 `tenant_access_token`。这是敏感凭据，放在 Hermes 受管的 0600 环境中。 |
| `FEISHU_AILY_TARGET_APP_ID` | Aily 平台的智能伙伴 App ID，通常形如 `spring_...`（有些租户带 `__c` 后缀）。它是问答 URL 的路径参数，不是调用方应用 ID。 |

可选 `FEISHU_AILY_DOMAIN` 为 `feishu`（中国区，默认）或 `lark`（国际版）。

官方接口同时接受 `tenant_access_token` 与 `user_access_token`；当前 Hermes tool 和 smoke 有意只实现 tenant 应用身份。完成以下前置条件后再执行网络 smoke：

1. 撤销/轮换曾经出现在聊天记录或 session 中的旧凭据；不要复用历史粘贴值。
2. 给调用方应用开通官方要求的 `aily:knowledge:ask`。
3. 打开 Aily 的“支持使用应用身份调用 API 和 SDK”。tenant token 不能查询直连模式引入的飞书云文档。
4. 本租户历史联调还要求将准确版本发布到“飞书机器人”渠道；这是针对现场 `2320008` 的运维经验，不是该问答接口页面列出的通用契约。

## API 契约

接口是：

```text
POST https://open.feishu.cn/open-apis/aily/v1/apps/:app_id/knowledges/ask
```

请求头和请求体：

```json
{
  "message": {"content": "问题文本"},
  "data_asset_ids": ["可选的数据知识 ID"],
  "data_asset_tag_ids": ["可选的分类 ID"]
}
```

`message.content` 长度为 1..65535。响应是 `text/event-stream`，每个事件以 `data: JSON` 开始；实现必须等待 `status=finished`。`processing.message.content` 是截至当前的完整快照，不是 delta，且不能在没有 finished 的情况下当作最终答案。`finish_type=faq` 时可从 `faq_result.answer` 取 FAQ 答案；`has_answer=false` 表示调用完成但没有由配置知识生成的答案，不能把它标作已验证知识答案。

即使 HTTP 状态为 200，也要检查事件或 JSON 中的业务 `code`。常见错误：

| code/HTTP | 含义与处理 |
| --- | --- |
| HTTP 400 / `2700001` | 参数错误，检查问题和目标 App ID。 |
| `2700033` | 问答失败，检查应用发布和知识配置。 |
| `2700034` | 无权限，检查 `aily:knowledge:ask` 与 Aily 应用身份开关。 |
| `2700035` | 运行超时，缩小知识范围或稍后重试。 |
| `2320008` | 本租户历史 smoke 曾观察到；官方问答页未定义该码。保留原始 `msg`/log id，并核对目标 ID、发布渠道和应用身份绑定。 |
| HTTP 429 / `99991400` | 触发频控；按 `x-ogw-ratelimit-reset` 等待后再做有界重试。该接口上限为 100 次/分钟。 |

## Hermes toolset

工具名为 `feishu_aily_knowledge_ask`，只接受问题和可选知识范围；目标 App ID 不暴露为模型参数。toolset 只能显式启用到 `cli` 或 `feishu` 平台：

```text
/tools enable feishu_aily
```

缺少 dedicated Aily 凭据时，Feishu comment 上下文可以使用显式注入的现有客户端；CLI/普通 gateway turn 应配置上表中的 dedicated 三项。工具只返回受限的最终文本、`has_answer`/`grounded`、`answer_available`、`finish_type` 和 FAQ 匹配问题，不把原始 `chunks`、SQL 或图表 DSL 回传给模型。

Hermes tool 通过 `lark-oapi` 的 raw request 读取完整响应后再解析 SSE，只承诺最终答案，不提供 processing 事件的实时 UI。下面的 smoke 使用流式 HTTP 行迭代，但同样只在 `finished` 后判定成功。

## 受控 smoke

使用 `LIVE_MANIFEST.json` 的 `runtime_venv` 对应 Python（当前观测值为 `/Users/songying/.hermes/runtime/venvs/hermes-v0.18.2-b85e919-sealed/bin/python`）。脚本读取 `HERMES_HOME/.env`（只读解析）和同名进程环境，不接受 secret 命令行参数，也不会改写 `.env`。它只验证 endpoint、目标与凭据组合，不等价于 resident handler、profile secret scope 或完整 gateway 路径的 canary。

先做无网络配置检查：

```bash
/Users/songying/.hermes/runtime/venvs/hermes-v0.18.2-b85e919-sealed/bin/python \
  scripts/feishu_aily_knowledge_smoke.py --pretty
```

确认飞书侧前置条件后，显式执行一次问答：

```bash
/Users/songying/.hermes/runtime/venvs/hermes-v0.18.2-b85e919-sealed/bin/python \
  scripts/feishu_aily_knowledge_smoke.py --execute \
  --question-stdin --pretty < /path/to/protected-question.txt
```

返回字段只包括 HTTP 状态、响应类型、可用于排查的 log id、finish 状态、`has_answer`、截断后的答案和 evidence 数量；不会打印 token、secret、完整 SSE 或原始知识片段。退出码 `0` 表示收到 `finished`，`2` 表示配置/输入错误，`3` 表示网络或 API 错误。

## lark-cli 边界

官方 CLI 的通用 raw API 可以发请求：

```bash
/usr/local/bin/lark-cli api POST \
  "/open-apis/aily/v1/apps/${FEISHU_AILY_TARGET_APP_ID}/knowledges/ask" \
  --as bot \
  --data '{"message":{"content":"问题文本"}}'
```

当前 `lark-cli api` 会缓冲非 JSON 响应，把 `text/event-stream` 当作文件处理，不是 SSE-aware 的答案客户端；稳定问答应使用上面的 Python smoke 或 Hermes tool。不要把调用方 CLI App ID 替换到 URL 的 `app_id`。

## 生成文档的边界

Aily tool 只负责问答，不自动创建或更新飞书文档。用户明确要求沉淀时，先取得写入授权，再把最终答案交给已验证为可写的飞书文档 MCP，或使用 `lark-cli docs +create`，写后执行 readback 校验。这样可以避免一次问答隐式产生外部写副作用。

## 发布顺序

离线测试和 endpoint smoke 收到真实 `finished` 事件后，还要通过候选 handler 与目标 profile secret scope 的真实 canary；随后才可以按 Hermes release governance 将候选提交 materialize 到 release，更新 `LIVE_MANIFEST`，重启 `ai.hermes.gateway`，并做 Feishu route canary。当前 live release 未包含此 toolset，不能直接修改 active runtime。

官方参考：

- [Aily 数据知识问答](https://open.feishu.cn/document/aily-v1/data-knowledge/ask?lang=zh-CN)
- [租户访问凭证](https://open.feishu.cn/document/server-docs/authentication-management/access-token/tenant_access_token_internal)
- [Lark CLI raw API](https://github.com/larksuite/cli#three-layer-command-system)
