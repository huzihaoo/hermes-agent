# 飞书 Aily Agent 企业知识接入测试报告

> 观测日期：2026-08-20 至 2026-08-21  
> 候选分支：`codex/feishu-aily-knowledge-qa-current-20260813`  
> 实现基线（本报告整理前）：`368e3a2176`

候选是累积实现，不能在后续移植时只 cherry-pick 最后两笔。完整 Aily
依赖链是：

```text
a1e4565ec7 -> e25ba59684 -> 4a0adaba91 -> dda09b7042
             -> 4b0623221d -> 70b15406e0 -> 368e3a2176
```

正式 RCA 分支就绪后，应以 Aily 文件/hunk manifest 审核移植，不应盲目
重放候选的分叉历史。候选与 2026-08-21 live gateway 的 merge-base 为
`93d26da4`。

## 结论

候选代码和隔离 UAT 会话已经证明以下链路可用：

1. Hermes 可以通过隔离的 `lark-cli` profile，以固定员工的
   `user_access_token` 调用 Aily Agent Chat。
2. create 和 poll 全程使用同一个用户身份；只有 `status=Completed` 才接受结果。
3. 飞书会话使用跨应用稳定的 `union_id` 做授权绑定。错误用户、缺失会话身份、
   CLI/cron/API 等非飞书入口均在发送问题前失败关闭，且不回退应用身份。
4. 问题从 stdin 进入 smoke，默认回执不包含问题、答案正文、token、secret、
   `open_id` 或 `union_id`。
5. `OOI是什么?` 的真实调用已返回内部 OOI 业务标记；其来源和
   grounding 尚未由持久化检索证据证明。

该候选**尚未部署到生产 gateway**。2026-08-21 的只读发布预检发现候选与当前
gateway commit `dcb535677` 已分叉，直接物化会回退大量 RCA 生产变更，因此发布被
主动停止。必须等待 RCA 正式生产分支，再只移植 Aily 相关改动并重新回归。

## 自动化测试

### 主回归

最近一次候选主回归覆盖 12 个相关测试模块：

```bash
uv run --frozen pytest -q \
  tests/tools/test_feishu_aily_agent_user_transport.py \
  tests/scripts/test_feishu_aily_agent_user_smoke.py \
  tests/tools/test_feishu_aily_agent_tool.py \
  tests/scripts/test_feishu_aily_agent_smoke.py \
  tests/gateway/test_session_env.py \
  tests/gateway/test_session_context_inheritance.py \
  tests/tools/test_local_env_session_leak.py \
  tests/hermes_cli/test_tools_config.py \
  tests/hermes_cli/test_tools_disable_enable.py \
  tests/hermes_cli/test_toolset_validation.py \
  tests/test_toolsets.py \
  tests/tools/test_registry.py
```

2026-08-21 在当前文档改动上重新执行，结果：`395 passed`。

独立只读复核另外运行了 250 个相关测试，未发现 P0/P1/P2。以下静态检查也通过：

- Ruff
- `py_compile`
- `plugin.yaml` 解析
- `git diff --check`

### 关键覆盖

| 边界 | 已验证行为 |
| --- | --- |
| 身份 | `open_id` 与 `union_id` 同时精确核对；姓名不作为授权键。 |
| 飞书入口 | 仅绑定的飞书会话可调用用户模式；其他平台和无绑定上下文拒绝。 |
| 密钥 | Hermes 不读取 UAT；子进程环境不转发 App Secret/UAT/代理和 Hermes/OpenClaw workspace 信号。 |
| 请求 | POST body 走 stdin，问题不进入 argv；create/poll 固定 profile 且显式 `--as user`。 |
| 进程 | 总 deadline、输出上限、匿名临时文件、独立进程组；超时/超限时 kill 整个进程组并 wait。 |
| 状态机 | `Queued`/`Running` 继续；仅 `Completed` 成功；`Failed`/`Cancelled` 失败关闭。 |
| 内容 | 只折叠相邻且完全相同的顶层文本；合法的非相邻重复内容保留。 |
| 配置 | `FEISHU_AILY_AUTH_MODE` 缺失或非法时失败；user/tenant 分别检查各自必需项。 |
| 隔离 | `union_id` 只存在于进程内 ContextVar，不进入 terminal、background、plugin 或 MCP 子进程环境。 |
| 文档 smoke | `.env` 按数据解析、白名单读取、0600/owner/regular-file/no-symlink 校验；不执行 `source`。 |
| 发布文档 | staging 命令清空继承环境并固定绝对 home/config/env 路径，不会误写 active home。 |

## 真实 canary

真实 canary 使用固定员工 UAT 和同一个 Aily Agent，问题为 `OOI是什么?`。以下
数字来自原始 2026-08-20 任务 session 的脱敏输出；当时未输出答案正文或
token：

| canary | API 状态 | 身份 | 答案 | 轮询 | 安全证据 |
| --- | --- | --- | --- | ---: | --- |
| 内部标记检查 | `Completed` | 已核验 | 2994 字符 | 16 | 同时包含 `Object of Interest` 及内部业务词组之一。 |
| 默认安全 smoke A | `Completed` | 已核验 | 2553 字符 | 17 | 默认回执无答案正文。 |
| 默认安全 smoke B | `Completed` | 已核验 | 2626 字符 | 14 | 默认回执无答案正文。 |

这些 session-derived attestation 表明当时观察到了“UAT -> Agent Chat ->
企业内部 OOI 内容”
的完整调用。task-work 没有持久化对应的脱敏 receipt，因此当前 workspace 不能独立
重放或审计这三条历史回执；正式发布必须重新执行并落不可变安全 receipt。它们也不
单独证明每段文本都由知识检索工具产生；`Completed` 和非空答案不能替代 Aily 已发布
配置、知识工具活动日志或检索来源证据。

## 尚未验证或尚未实现

1. **生产 resident 未启用**：active config 中 `feishu_aily_agent` 仍关闭，gateway
   未重启到候选代码。
2. **正式 RCA 分支集成未完成**：必须在正式分支上解决共享文件冲突后重跑生产分支
   回归，不能直接发布当前候选 commit。
3. **自动检索路由未实现**：当前工具可供模型显式调用，但“关键词/任务上下文自动
   触发 Web、本地或企业知识检索”仍是设计目标。
4. **结构化 grounded 证据不足**：当前 Agent Chat 返回文本、状态和产物标识，没有
   统一的检索来源/命中片段契约。任务流程不能把 `answer_available=true` 当作
   `grounded=true`。
5. **真实飞书端到端 canary 待发布后执行**：已有 UAT API canary，但尚未证明生产
   gateway 收到固定员工消息后自动选择该工具。
6. **多用户授权未实现**：当前故意固定单个员工。扩展前必须设计每用户独立凭据、
   稳定身份映射和并发隔离，不能共享固定员工 UAT。
7. **性能基线不完整**：回执记录 poll 次数，尚未形成 p50/p95 总耗时、无匹配率、
   超时率和缓存收益的正式基线。
8. **无人值守 RCA 身份未实现**：固定员工 UAT 只允许对应飞书会话使用。Kafka/outbox
   自动任务没有该会话身份，不能借用固定员工权限；必须另建受审的只读 service
   principal/provider。该增强尚未实现不会阻断现有 RCA，查询失败后的正式策略也是
   继续原链。
9. **grounding 负例未完成**：尚未持久化 Aily 活动日志/来源，也没有“知识
   未命中且确认未走公网”、ACL 拒绝和 resident Feishu E2E receipt。当前真实
   答案只能归类为 `answer_only`。

## 旧 Data Knowledge API 隔离结果

旧 `spring_...` Data Knowledge API 与新 `agent_...` Agent Chat 是两条不同链路。
历史 Data Knowledge live probe 收到 HTTP 200 + 业务码 `2320008`，没有进入
SSE `finished`，因此必须记为失败，不属于上述 Agent UAT 成功证据。旧接口只作
兼容候选，本任务的企业知识主路径是 Agent Chat。

## 发布验收门

等待 RCA 正式生产分支后，至少满足以下条件才能发布：

1. 只移植 Aily 相关变更，证明没有删除或回退现有 RCA 文件与契约。
2. 本报告的主回归在正式分支全部通过，并补充统一检索路由和 RCA 接入测试。
3. 形成结构化 `business_knowledge_context`，明确 `grounded_match`、
   `answer_only`、`no_match`、`identity_unavailable`、`timeout`、`error`、
   `not_required`，并证明所有失败状态继续原 RCA。
4. 完成 governed materialize、manifest/config/env 指纹、gateway 新 PID 和平台连接回读。
5. 在固定员工真实飞书会话中验证：业务关键词自动触发 Aily；通用问题不触发；
   RCA 无论是否显式包含关键词都尝试企业知识增强，失败后继续原链。
6. 保存不含内部答案正文和个人标识的发布 receipt，并保留 Aily 知识工具策略/活动
   证据，证明没有公开网络兜底。
7. 对 Kafka RCA 使用独立、只读、受审的后台身份；固定员工 UAT 不进入 resident、
   outbox worker 或 VM。
8. 用基线 oracle 证明增强关闭或任何 provider 故障时，原 v2 request/hash、core result、
   outbox、VM submit、报告和投递行为不变。
9. 二阶段补查只允许 VM 发有界 gap artifact、host 查询并写 owner-only addendum；
   主 task 不等待、不 resume，且不得进入人工 `need_input`、创建新 RCA 或阻断原投递。
