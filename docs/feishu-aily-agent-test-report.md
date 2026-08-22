# 飞书 Aily Agent 企业知识接入测试报告

> 观测日期：2026-08-20 至 2026-08-21
> 候选分支：`codex/feishu-aily-knowledge-qa-current-20260813`
> 实现基线（本报告整理前）：`368e3a2176`

候选是累积实现；以下历史链只用于定位已测试的 transport/tool/identity 代码：

```text
a1e4565ec7 -> e25ba59684 -> 4a0adaba91 -> dda09b7042
             -> 4b0623221d -> 70b15406e0 -> 368e3a2176
```

候选与 2026-08-21 live gateway 的 merge-base 为 `93d26da4`。当日 owner 决定是不把
这条分叉历史盲目重放到当时的 RCA 业务分支。2026-08-22 owner 已更新
源码发布边界：可以把 exact range `9b701912..6948120a` 的功能内容重放到
当前正式 Host 源码，但明确排除旧基线 `9b701912`。该更新只授权源码合入：
Aily toolsets、provider、consumer seam 和凭据仍保持 default-off，不授权真实飞书调用、
gateway 重启、运行时激活或 RCA 产物回写。

## 结论

候选代码和隔离 UAT 会话已经证明以下**交互候选/API**链路可用：

1. Hermes 可以通过隔离的 `lark-cli` profile，以固定员工的
   `user_access_token` 调用 Aily Agent Chat。
2. create 和 poll 全程使用同一个用户身份；只有 `status=Completed` 才接受结果。
3. 候选飞书路径使用跨应用稳定的 `union_id` 做会话授权绑定。错误用户、缺失会话
   身份、CLI/cron/API 等非飞书入口均在发送问题前失败关闭，且不回退应用身份。
4. 问题从 stdin 进入 smoke，默认回执不包含问题、答案正文、token、secret、
   `open_id` 或 `union_id`。
5. `OOI是什么?` 的真实调用已返回内部 OOI 业务标记；其来源和
   grounding 尚未由持久化检索证据证明。
6. 2026-08-21 本机独立进程在没有飞书 session context 时通过 user smoke 完成固定
   UAT 调用，返回 `user_identity_verified=true`。这证明 standalone 后台 transport
   能力，不证明 RCA observer 已接入或其唯一调用面已经实现。

该证据**不证明自动本机 RCA observer 已实现**。手动 shadow planner 原型虽已存在，但
尚未启用、未注册、不是 daemon，也没有 consumer seam；后台自动调用仍需接入 observer
专用隔离 broker，在每次 create 前重复精确核验 token 对应的胡子豪 `open_id + union_id`，
同时禁止通用对话、其他用户、CLI/API、业务 worker 和 VM 借权。

该候选**尚未部署到 production gateway**。2026-08-21 的只读预检发现候选与当时
gateway commit `dcb535677` 已分叉，直接物化会回退大量 RCA 生产变更，因此操作被
主动停止。2026-08-22 的合入必须从当前正式 Host 基线重放 exact feature patch，
并重新生成 commit/tree-bound 证据。源码进入正式分支不表示 toolset/provider 已启用；
本机 shadow planner 仍位于 Hermes owner-only 路径，其 consumer seam、凭据和生产效果
仍需单独审批与验证。

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

候选基线的历史回归结果记录为 `395 passed`；这不是本轮最终测试数字，也不代表 shadow
planner 或自动 observer 已验收。

候选阶段的独立只读复核另外运行了 250 个相关测试，未发现 P0/P1/P2；这同样是历史记录，
不是本轮最终测试数字。以下静态检查也通过：

- Ruff
- `py_compile`
- `plugin.yaml` 解析
- `git diff --check`

本轮修改设计、交接和合同断言，没有修改候选 transport/tool/runtime 实现。相关合同测试、
Markdown 链接检查、owner 决策残留 `rg` 扫描和 scoped `git diff --check` 的实际结果应
以本轮命令输出为准。当前可复核结果如下：

- 本机 shadow planner：`10 passed`，使用 `/usr/local/bin/pytest`，无网络。
- 候选文档/合同测试：`37 passed`，使用候选 worktree 的 Python 3.11 环境，无网络。
- Aily Agent/knowledge 离线相关测试集合：`149 passed`，无网络、无真实 API 请求。
- 2026-08-22 统一 Host 候选的 25 个 changed-test 文件：`1430 passed, 1 skipped`，
  无网络、无真实飞书请求。验证命令显式清除了调用环境中的
  `HERMES_CONFIG_PATH`/`HERMES_ENV_PATH`，使测试继续使用每用例的隔离
  `HERMES_HOME`；未清除时唯一失败是当前正式基线也存在的 test-fixture
  环境绑定问题，不是本候选运行时回归。
- 两组测试均使用 `-p no:cacheprovider` 和 `PYTHONDONTWRITEBYTECODE=1`。
- `py_compile`、候选 `git diff --check` 和所有 changed Python 文件 Ruff 检查通过。
- live `inspect --latest-completed`：`triggered=false`、`query_kind=not_triggered`、
  `requirement=not_required`；显式 stdin 的 OOI dry-run 仅返回 `status=planned`，未调用
  Aily、未写 observer 状态。
- `report_followup:1` 当前对 live shared-state 结果 fail closed（缺少可验证的 sealed
  delivery manifest/contract）；这不是成功的二阶段能力证明，后续需 DB sealed join 或
  明确 sealed fixture。

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

以上覆盖描述的是当时的候选代码，不是新 observer 的验收结果。尤其是“飞书入口”测试
不能替代后台 broker 的 token 身份核验，“发布文档”测试也不表示首版需要或允许发布
gateway。

## 真实 canary

真实 canary 使用固定员工 UAT 和同一个 Aily Agent，问题为 `OOI是什么?`。前三行数字
来自 2026-08-20 原始任务 session 的脱敏输出，最后一行来自 2026-08-21 无飞书 session
context 的本机独立调用；两组默认输出都没有答案正文、token 或 secret：

| canary | API 状态 | 身份 | 答案 | 轮询 | 安全证据 |
| --- | --- | --- | --- | ---: | --- |
| 内部标记检查 | `Completed` | 已核验 | 2994 字符 | 16 | 同时包含 `Object of Interest` 及内部业务词组之一。 |
| 默认安全 smoke A | `Completed` | 已核验 | 2553 字符 | 17 | 默认回执无答案正文。 |
| 默认安全 smoke B | `Completed` | 已核验 | 2626 字符 | 14 | 默认回执无答案正文。 |
| 本机 standalone user smoke | `Completed` | 已核验 | 2300 字符 | 17 | 无飞书 session context；3 个文本项、0 个产物；默认回执无答案正文/token/secret。 |

前三条 session-derived attestation 表明当时观察到了“UAT -> Agent Chat -> 企业内部
OOI 内容”的完整调用。task-work 没有持久化对应的脱敏 receipt，因此当前 workspace
不能独立重放或审计这三条历史回执；本机 observer 启用前必须重新执行并落 owner-only
安全 receipt。所有 canary 都不能单独证明每段文本由知识检索工具产生；`Completed` 和
非空答案不能替代 Aily 已发布配置、知识工具活动日志或检索来源证据。

standalone 成功 canary 的 safe result 为 `ok=true`、`status=Completed`、
`user_identity_verified=true`、`poll_count=17`、`answer_available=true`、
`answer_length=2300`、`text_item_count=3`、`artifact_count=0`；未记录实际 app、agent 或
user ID 到下面的脱敏 receipt。原始 smoke 标准输出仍包含配置的 app/agent 标识，因此
只能作为 owner-only 终端输出处理；它不包含 user ID、token、secret 或答案正文。它之前
有一次 PTY EOF 操作在已经进入 poll 后被人工中断，没有终态，因此既不计入成功也不
伪记为 API 失败。上表成功证据来自随后使用非交互 stdin 的独立调用。

该 standalone 成功调用的脱敏 receipt 已保存为
`/Users/songying/.codex/task-work/20260820-140527-aily/aily-agent-fixed-uat-background-canary-20260821T073325Z.json`，
文件 mode 为 `0600`，SHA-256 为
`141333d61d6efe2636f9d4bb48ccf1ff5e6df3a9f8ad32d4f112599a4d1218f0`。它是本机
`task-work` 下的历史 canary 证据，不是 production/resident evidence，也不证明 RCA
observer 已接入、grounding/source provenance 或 no-public-web。

## 尚未验证或尚未实现

1. **自动本机 observer 未实现/未启用**：手动 shadow planner 文件和 wrapper 已存在，
   但未注册、非 daemon、没有 consumer seam，不能回灌正在执行的 RCA。
2. **确定性 trigger 尚未接入自动消费**：只有注册业务术语或 stdin 显式查询才应触发；
   无信号（包括仅 `task_type=rca`）必须为 `not_required`/原型 `not_triggered`，不调用
   Aily 或 Web。
3. **真实 provider 当前禁用/不可用**：`inspect` 和 `--provider dry-run` 不联网；现有
   一次性 transport 没有耐久 create/poll 恢复，不能宣称真实增强已上线。
4. **completed report 二阶段未实现**：必须先有 sealed delivery manifest 与 delivery
   contract；缺失、无效或身份/hash 不匹配时 fail closed。当前尚无只读完成态判定、
   确定性新注册术语提取、
   排序去重、query allowlist 或至多一轮两条补查 query。最新方案不要求 VM gap 或业务
   侧 schema/addendum，但未来只允许本机 host-private reference addendum。
5. **结构化 grounded 证据不足**：当前 Agent Chat 返回文本、状态和产物标识，没有
   统一的检索来源/命中片段契约。任务流程不能把 `answer_available=true` 当作
   `grounded=true`。
6. **本机 observer 真实 canary 未执行**：已有固定 UAT API canary 和 standalone smoke，
   但尚未证明自动 observer 能只读 RCA、按确定性信号构造问题、调用 broker 并只写
   owner-only 状态。
7. **多用户/通用对话不在首版范围**：固定 UAT 只允许本机 RCA observer 使用；扩大前
   必须重新设计逐用户凭据、稳定身份映射和并发隔离，不能沿用本次风险接受。
8. **性能基线不完整**：回执记录 poll 次数，尚未形成 p50/p95 总耗时、无匹配率、
   超时率和缓存收益的正式基线。
9. **本机写入边界未验证**：尚无 before/after oracle 证明业务仓、VM、业务 schema、
   artifacts、原报告和投递为零改动。
10. **grounding 负例未完成**：尚未持久化 Aily 活动日志/来源，也没有“知识未命中且
    确认未走公网”的后台策略或活动证据。当前真实答案只能归类为 `answer_only`。

Owner 已明确接受并延后固定 UAT 带来的权限代理、审计归属、ACL 漂移和 token 生命周期
风险；专用服务身份、细粒度 ACL 和审计映射因此不列入上述首版缺口。该风险接受不覆盖
身份错配、token 泄露、通用对话借权、VM 下发或写入业务产物。

## 旧 Data Knowledge API 隔离结果

旧 `spring_...` Data Knowledge API 与新 `agent_...` Agent Chat 是两条不同链路。
历史 Data Knowledge live probe 收到 HTTP 200 + 业务码 `2320008`，没有进入
SSE `finished`，因此必须记为失败，不属于上述 Agent UAT 成功证据。旧接口只作
兼容候选，本任务的企业知识主路径是 Agent Chat。

## 本机 observer 验收边界

这里不是正式 RCA 分支或 production 发布授权。后续在本机显式启用 observer 前，至少
取得以下新证据；历史候选测试和 API canary 只能复用为参照：

1. observer/broker/配置/状态全部位于 owner-only Hermes 本机范围；业务仓、VM、业务
   schema/artifact、原报告和投递的 before/after oracle 为零改动。
2. broker 每次 create 前精确核验胡子豪 `open_id + union_id`；错误或缺失身份在发送
   问题前失败，不回退 TAT，UAT/App Secret 不出现在 observer 环境、argv、日志或 receipt。
3. 只有本机 RCA observer 能调用 broker。通用 Hermes/飞书对话、其他用户、CLI/API、
   Kafka/outbox、dispatcher 和 VM 的负例都在网络请求前拒绝。
4. 第一阶段只有确定性注册术语或 stdin 显式查询才触发；无信号返回
   `not_required`/`not_triggered`，不调用 Aily 或 Web。触发后的 query 只使用允许字段并
   有界；完整 issue/report、ID、URL、PDCL、帧、日志、用户/评论、附件和现有 root cause
   不进入 Aily。
5. completed report 只有在 sealed manifest/contract 绑定通过时才只读完成态、确定性新
   注册术语 extractor、固定排序/去重/上限和
   prompt-like 文本负例通过；第二阶段最多一轮两条 query，不产生 VM gap、业务 schema
   或 artifact。
6. timeout、403、429、5xx、no-match、bad JSON、oversize、crash、create-unknown 和本地
   存储失败都只终止 observer job，不改变原 RCA 或触发 retry/delivery。
7. 形成 owner-only safe receipt；不含 query、answer、user、chat、session、token，低熵
   digest 使用 keyed HMAC 或不持久化。无来源答案只标 `answer_only`。
8. 以已成功的 standalone user smoke 为 transport 基线；只有在真实 provider 明确启用、
   consumer seam 放开且耐久 create/poll 完成后，才可通过 observer 执行端到端 canary。
   当前不能宣称 observer 已帮助 RCA，且候选通用 toolset 和 production gateway 仍未启用。

以上技术证据不能代替绑定 exact Host commit/tree、配置、凭据作用域和允许 effects
的独立启用授权。

专用服务身份、ACL 正反 canary、审计触发者映射和 token 轮换自动化属于后续专项，不是
首版本机启用门。通过以上验收只能声称“本机 observer 技术证据完成”；在
独立授权和效果回读完成前，不能声称运行时能力已进入 VM、production gateway
或正式 RCA 激活面。
