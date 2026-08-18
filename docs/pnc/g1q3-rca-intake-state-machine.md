# G1Q3 RCA Intake 状态机与主控字段契约

> 本文分两部分：前半部分是**当前有效口径（current state）**，后半部分是**迁移历史附录（superseded）**。
> 新接手 agent 只需要读 current state；附录仅用于审计和回溯，不得作为当前链路依据。
> 最后核对时间：2026-07-11。生产采用 Kafka 自动创建 + 受控群 @ 明确动作双入口；两者共享同一执行链。问题数据只远程读取，不执行 MDI。

## 目标

G1Q3 RCA 可由 Feishu 问题创建 Kafka event 自动触发，也可由两个固定业务群的当前 active subset 内真实 `@小助手` 加明确动作和唯一 canonical issue identity 触发。active subset 由 `HERMES_RCA_MANUAL_CHAT_IDS` 显式给出，必须是固定群的非空子集；总闸开启但 subset 为空仍 fail closed。普通用户的分析/紧急分析只能 `run_or_join`；`rerun/debug` 还要求 canonical `HERMES_RCA_MANUAL_OPERATOR_ENABLED`、非空 `HERMES_RCA_MANUAL_OPERATOR_USER_IDS`、授权 receipt，以及显式正整数 `HERMES_RCA_MANUAL_OPERATOR_RATE_LIMIT=3` / `HERMES_RCA_MANUAL_OPERATOR_RATE_WINDOW_SECONDS=600` durable 限速。production 开启 manual intake 时 operator 开关必须显式配置；旧 `HERMES_RCA_MANUAL_DEBUG_*` 仅作 canonical 值缺失时的兼容 fallback。两类入口先写 source/binding/business-generation/outbox；主控随后完成 issue 字段读取、字段固化与基础校验，再通过 `g1q3_rca_execution_request_v2` 和 capability-scoped service 传给 VM。VM 只消费结构化字段和 blocker，不反向猜测飞书卡片状态。

人工入口不等于旧群聊 handoff：未 @、只贴 URL、问状态/进展、私聊或非固定群均只读；任何人工执行都必须进入 durable outbox，不能直提 VM 或通用 Agent。

人工命令只从当前消息读取执行意图。issue identity 可以来自当前消息、结构化卡片链接元数据或被回复卡片；回复正文只能补 identity，不能把其中的 `debug/重跑` 当命令。多个不同 identity 必须澄清后重发；同一 canonical URL 重复出现仍视为一单。Feishu message inbox 只有在 callback 完成 durable admission 后才从 `processing` 置为 `completed` 并推进 poll cursor，异常/进程崩溃后允许同一消息重投。

## 主控状态

- `admitted`：Kafka 或人工 source 已持久化并绑定 generation。
- `issue_enrichment_started`：开始读取飞书 issue 字段/评论。
- `issue_preread_blocked`：主控未成功读取飞书 issue 字段/评论，或读取返回空；此状态不能判定业务字段缺失。
- `issue_fields_extracted`：主控已提取飞书 issue 字段，并写入结构化 request。
- `issue_field_validation_blocked`：主控已读取卡片，但固化字段缺失或格式不满足契约。
- `vm_submitted`：结构化 request 已提交给 VM 执行。

保留旧状态 `issue_enriched` / `issue_enrichment_blocked` 只为兼容旧 receipt；新链路应使用上面的精确状态。

## 字段契约

主控传给 VM 的 `data.data_access` 必须显式包含：

```json
{
  "schema_version": "g1q3_rca_remote_data_access_v1",
  "mode": "remote_read",
  "transport": "pdcl_pyclip",
  "references": [
    {
      "kind": "event",
      "event_uuid": "<event_uuid>",
      "reader_class": "RemoteEventReader"
    }
  ],
  "source": {
    "field": "问题数据地址_PDCL",
    "value_sha256": "<64 lowercase hex>"
  },
  "reader_contract": {
    "distribution": "pdcl_pyclip",
    "required_version": "0.1.6+rca.2",
    "mdi_download_allowed": false,
    "fallback": "forbidden",
    "completeness": "full_requested_scope"
  }
}
```

Ready 对象没有 `status` 字段；只有 Host 已证明引用无效时的内部 blocked 对象带 `status=blocked`，且不能交给 VM 执行。clip 引用使用 `clip_uuid` + `RemoteClipReader`。ABI 是 exact-shape：未知字段、通用 `resource_id`、缺少 source hash 或 reader class 均拒绝。

当主控未读到飞书卡片时：

- `evidence.source_quality = "unavailable"`
- `evidence.blockers[].kind = host_issue_preread_failed | host_issue_preread_empty | host_issue_preread_unavailable | host_meegle_preread_*`
- VM/群回传必须说明“主控侧未成功读取飞书 issue 字段/评论”，不能说“问题数据地址_PDCL 缺失”。

当主控已读到卡片但远程引用字段有问题时：

- 缺失：`issue_field_missing_remote_data_reference`
- 格式不合法：`issue_field_invalid_remote_data_reference`
- 此时才允许群回传说 `问题数据地址_PDCL` 缺失或格式不合法。

## 当前读取链路（current state，2026-06-11 起生效）

默认链路：**Meegle primary / MCP bounded auto-degrade**。

1. 主链路：`meegle auth status` → `meegle workitem get --fields _all` → `meegle comment list`，实现在 `gateway/pnc_issue_context.py`。
2. `meegle workitem get` 必须传 `--fields _all`，否则不会返回 `问题数据地址_PDCL` 等自定义字段。
3. Meegle 读取失败时，`HERMES_G1Q3_MCP_AUTODEGRADE=1`（默认）允许受界 MCP 只读降级；`=0` 可强制关闭。`HERMES_G1Q3_MCP_FALLBACK=1` 仅用于显式诊断。成功降级必须在 execution evidence 中记录 `source=mcp_auto_degraded`、`degraded=true` 和脱敏错误类别，不能伪装成 Meegle 成功。
4. 旧开关 `HERMES_G1Q3_MEEGLE_FALLBACK` 已废弃，不再是生产开关。
5. 范围边界：仅作用于 G1Q3 RCA / Feishu Project issue 预读取；不全局禁用 Hermes MCP，其它业务线仍可按各自路径使用 MCP。

### Meegle 登录态与过期处理

- Meegle token 有过期时间。gateway 启动时会做 best-effort 预检（`check_meegle_auth_status`，见 `gateway/run.py` 的 `start()`）：未登录或剩余 ≤30 分钟会打 warning 日志，提示重新 `meegle auth login --device-code`。
- intake 运行期：Meegle 未登录/过期归类为 `host_meegle_preread_unauthenticated`（preread blocker），群内接单回复会附加“Meegle 登录已过期/未授权，请重新授权”提示；**禁止**转换成 PDCL 缺失/非法。
- 登录恢复命令：

```bash
meegle config set host project.feishu.cn
meegle auth login --device-code --host project.feishu.cn
meegle auth status --format json
```

### Blocker 投影与只读查询

Kafka 自动链不发送群聊“接单即创建”回复；人工群 @ 只发送持久化 admission 的追踪号/代次确认，并明确“成功时 HTML、失败时终态说明”都会回当前话题。issue preread 的 `status / source / source_quality / blocker` 先写入 durable outbox/执行证据，再由 required delivery effects 投影；不得从旧群聊路径直接写卡片或评论。所有成功 generation 写问题单评论；人工 source 还订阅原任务话题，报告完成后在该话题交付，禁止回退主群。

`terminal_failed` 与 VM 提交前 quarantine 也必须原子创建问题单与原话题 required failure effects，不伪造 HTML。pre-submit quarantine 的公开码固定为 `outbox_submission_quarantined`，原始 code/detail 仅保留在 DB。required effect 全部 settled 前禁止 rerun/debug；5/15/60 分钟 outcome SLO 或 1 小时内连续 3 次终态交付失败会背压新 outbox，但既有 delivery recovery 继续运行。真实成功/失败 marker 对账 canary 仍是 production blocker。

- `*unauthenticated*` → “Meegle 登录已过期/未授权……这不代表 问题数据地址_PDCL 缺失”。
- `issue_field_*` → “字段已读取，但 问题数据地址_PDCL 缺失或格式不合法，请补充 event UUID 或 clip UUID；系统只远程读取”。
- 其它 preread blocker → “主控侧本次未成功读取飞书 issue 字段/评论（issue_preread_blocked）”。

`_submit_g1q3_rca_status_handoff` 只保留兼容拒绝边界：issue intake 返回 `g1q3_rca_issue_intake_kafka_only`；状态/证据查询读取 control store 或既有任务，不预读 issue、不创建 VM/Codex 任务，也不写任何 receipt。

### fpx 辅助工具集（current state）

- 本地轻量 wrapper：`/Users/songying/bin/fpx`（`0.1.0-local`），复用官方 `meegle` CLI 登录，不保存 token。未安装 npm 裸包 `fpx`（那是无关 JS 库）。
- 已挂载 Hermes toolset=`fpx`：`fpx_capabilities` / `fpx_capability_get` / `fpx_workitem_get` / `fpx_comment_list` / `fpx_session_status` / `fpx_run_readonly`。
- `fpx_workitem_get` 支持 `compact=true`（只返回工作项摘要 + RCA 所需字段）和 `select=[字段名...]`；默认仍返回脱敏全量 JSON。Agent 调用优先用 compact/select。
- 安全边界：未注册写操作（comment create / workhour / MR create 等）；`fpx_run_readonly` 阻断 mutating token；输出递归脱敏 `email/key/user_key/open_id/creator/token/secret/password/authorization`。
- **fpx 不在 G1Q3 issue preread 热路径中**；热路径直接调用 Meegle。
- 回滚：删除 `/Users/songying/bin/fpx` 即可，不影响 Meegle 登录态和 gateway。

## 防漂移回归

Host 定向回归必须在绑定内部 GitLab commit 的 clean 开发 worktree 中运行；不要进入 production runtime 改代码或执行会生成文件的开发测试：

```bash
cd <clean-gitlab-worktree>
pytest -q -o addopts='' \
  tests/tools/test_fpx_tool.py \
  tests/gateway/test_pnc_issue_context.py \
  tests/gateway/test_pnc_rca_schema.py \
  tests/gateway/test_pnc_rca_state_machine.py \
  tests/gateway/test_pnc_group_binding_status_handoff.py
```

VM runner：

```bash
python3 -m py_compile /home/mini/data3/yj-evaluation-server/api/g1q3_rca/scripts/run_rca_execution_request.py
```

并用 `host_issue_preread_failed + source_quality=unavailable` 的 request 做 readback smoke，确认不再输出“问题数据地址_PDCL 缺失或格式不合法”。

定期 parity canary（建议 weekly/manual）：

```bash
python3 scripts/g1q3_issue_source_compare.py --work-item-id <id> ... --json
```

---

## 附录：迁移历史（superseded，仅供审计）

> 以下段落记录 2026-06-11 当天的演进过程。其中关于“MCP 是主链路”“Meegle 仅兜底”“fpx 未安装”的表述**均已失效**，被上文 current state 取代。

### 阶段 1：MCP 主链路 + Meegle 候选源准入门禁（已失效）

当时策略：默认主链路为 Hermes MCP `mcp_feishu_project_get_workitem_brief` + `mcp_feishu_project_list_workitem_comments`；Meegle CLI 仅作为诊断/显式开关兜底源（`HERMES_G1Q3_MEEGLE_FALLBACK=1`）。

准入门槛（已完成验收）：

1. 样本：至少 10 个近期/代表性 G1Q3 issue。
2. 必填字段通过率：`title`、`work_item_id`、`当前状态`、`问题数据地址_PDCL` 及 PDCL 格式校验 100%。
3. 期望字段回归：MCP 能读取的字段 Meegle 不得缺失。
4. 失败分类：认证/工具/网络失败不得误报为 PDCL 字段缺失。
5. 延迟满足群 intake p95；热路径不得触发交互式登录。

当天早些时候对 `7015689036` 的首次对比中，本机 Meegle 尚未登录（`no local token`），判定 `ok_to_switch_preferred_source_now=false`——这是当时未切主的原因，登录后已不成立。

### 阶段 2：Meegle 10 样本源一致性复测（验收依据，结论仍有效）

样本：`7015843358` `7015805715` `7015849914` `7015878869` `7016172537` `7015859334` `7015866910` `7015999473` `7015889365` `7015851762`（`t03o4q` 空间近期 issue，MQL 获取）。

结论：

- 源一致性 `10/10`，Meegle 相对 MCP 无字段/状态/PDCL 回归。
- MCP 有效 PDCL 样本 `2/10`；MCP 有效时 Meegle 有效 `2/2`。
- 延迟：MCP 平均约 `1863.8ms`，Meegle 平均约 `2871ms`，均可接受。
- 关键实现要求：`--fields _all` 必传。

报告文件：`outputs/pnc/g1q3-meegle-soak-10-20260611.json`

### 阶段 3：切主决策（即当前 current state 的来源）

根据阶段 2 复测结果，G1Q3 issue preread 默认链路切换为 Meegle primary / MCP explicit fallback（`HERMES_G1Q3_MCP_FALLBACK=1`）。真实验证：`7015689036` 在默认链路下未调用 MCP，Meegle 成功读取全部 RCA 字段，PDCL 有效。

### 阶段 4：fpx 从“未安装”到“本地 wrapper + Hermes 挂载”（已收敛进 current state）

- 早期评估时 `fpx` 未安装，G1Q3 intake 不依赖 fpx（该边界至今保留）。
- 后安装本地 wrapper `/Users/songying/bin/fpx`（`0.1.0-local`），验证 capabilities / workitem get（含 `--fields _all`）/ comment list / 隔离配置目录下 session 与 mr prepare。
- 再挂载为 Hermes toolset=`fpx`（6 个只读工具）。写操作工具需单独审批设计后才注册。
