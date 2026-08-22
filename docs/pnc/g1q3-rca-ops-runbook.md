# G1Q3-RCA 生产 Runbook

> 适用范围：Host `operator_issue_only_v1` 发布、读回、单 canary 和回滚。
> 当前事实以 live manifest、ControlStore、resident 进程/health、VM task receipt 和飞书字段读回为准。

## 1. 不可混淆的职责

| 对象 | 职责 |
|---|---|
| Host | Issue 读取、准入、outbox、VM 投递、结果收敛和飞书字段交付 |
| VM | RCA scheduler、实时资源 gate、fixed direct CLI 和结果回传 |
| VM 业务仓 | 归因领域实现与报告生成 |
| Codex/Claude | 开发、审查和发布运维；不执行生产 RCA 分析 |

Host release 不复制 VM 调度、资源控制或归因能力。VM 业务改动由业务仓自己的 GitLab release identity 管理，Host note 只绑定并读回该 identity。

## 2. 唯一生产定义

生产远端固定为：

```text
git@git.minieye.tech:planning_algo/hermes.git
```

每次发布只保留以下合同：

```text
exact GitLab branch/tag/commit/tree
+ owner-only minimal release note
+ immutable runtime
+ atomic ControlStore v14-to-v15 cutover and steady successor
+ four-face restart/readback
+ one transport canary
```

GitHub 仅作 Hermes 官方 upstream/reference，不参与 RCA production release，也不要求同步。

发布工具：

```text
scripts/pnc_rca_minimal_release.py
```

从当前 live manifest 解析受管 runtime Python；不要使用缺少生产依赖的系统 Python：

```bash
REPO_ROOT='/path/to/reviewed/hermes-release-worktree'
PY="$(jq -er '.runtime_python' "$HOME/.hermes/runtime/LIVE_MANIFEST.json")"
test -x "$PY"
cd "$REPO_ROOT"
```

禁止直接修改 `~/.hermes/runtime/releases/<release>/`，禁止从 mutable source 目录启动 resident，禁止手工写 live manifest、epoch 或 release binding。

## 3. 发布输入

不得手工拼 release note、candidate env 或 candidate manifest。使用同一个 driver 从语义输入生成三份 owner-only 候选文件：

```bash
"$PY" scripts/pnc_rca_minimal_release.py prepare \
  --release-id "$RELEASE_ID" \
  --epoch-id "$EPOCH_ID" \
  --operator "$OPERATOR" \
  --reason "$REASON" \
  --canary-batch-id "$CANARY_BATCH_ID" \
  --canary-issue-id "$CANARY_ISSUE_ID" \
  --canary-state-path "$CANARY_STATE" \
  --host-branch "$HOST_BRANCH" \
  --host-tag "$HOST_TAG" \
  --host-runtime-root "$HOST_RUNTIME_ROOT" \
  --worker-remote "$WORKER_GITLAB_REMOTE" \
  --worker-branch "$WORKER_BRANCH" \
  --worker-tag "$WORKER_TAG" \
  --worker-runtime-root "$WORKER_RUNTIME_ROOT" \
  --pipeline-remote "$PIPELINE_GITLAB_REMOTE" \
  --pipeline-branch "$PIPELINE_BRANCH" \
  --pipeline-tag "$PIPELINE_TAG" \
  --pipeline-runtime-root "$PIPELINE_RUNTIME_ROOT" \
  --report-manifest-path /home/mini/.config/g1q3-rca/report-runtime-manifest.json \
  --control-db "$CONTROL_DB" \
  --release-note "$RELEASE_NOTE" \
  --manifest-output "$MANIFEST_SOURCE" \
  --env-output "$ENV_SOURCE"
```

`prepare` 不接收 commit/tree/tag object、report SHA、partition offset 或 candidate hash。它从 GitLab exact branch/tag 派生三面 commit/tree/tag object，通过固定的 `~/.local/bin/ssh-mini-agent` 只读稳定 VM report manifest 并绑定原始 SHA，从只读 ControlStore snapshot 派生数据库 identity、v14 predecessor、partition fence 和 v14-to-v15 epoch contract，再从当前 live env/manifest 模板生成候选投影。`operator_issue_only_v1` 默认不启用 Kafka，因此 fence 可以为空；只有明确启用 Kafka 的后续 profile 才传 `--partition-topic TOPIC=PARTITION`。发布前会再次读回 GitLab、Host runtime 和 report manifest；任一漂移时不创建输出。当前 driver 只生成 exact v14-to-v15 cutover note；已有 v15 只允许同一 successor 的幂等读回，不能用它创建另一个 v15 epoch。

当常驻 writer 使 raw DB/WAL/SHM snapshot 在普通 prepare 前发生漂移时，先运行同一组参数的只读 `prepare-preflight`。它不会打开 ControlStore，也不会要求未来 canary；这一步只用来提前排除静态输入问题。随后在明确的维护窗口使用 `activate`，由 driver 在一个 release lock 内 quiesce 六个 resident，重新执行完整 `prepare`、`plan`、`apply`，并写入独立的 `--quiesce-receipt` 与 apply `--receipt`。prepare/plan 失败会按 receipt 中的原始 loaded/disabled profile 自动恢复；apply 一旦进入事务，沿用现有 receipt 的 `not_committed`、`committed` 或 `unknown` 语义，禁止自动猜测恢复。普通 `preflight`、`prepare`、`plan`、`apply` 命令的默认行为不变。

三个输出使用 `0600`、`O_EXCL` 和 fsync；任一输出已存在时拒绝覆盖。`prepare` 的 `templates` 给出 plan 所需的 live expected SHA，`outputs` 给出 candidate SHA：

```bash
CANDIDATE_MANIFEST_SHA='<prepare.outputs.manifest.sha256>'
CANDIDATE_ENV_SHA='<prepare.outputs.env.sha256>'
LIVE_MANIFEST_SHA='<prepare.templates.manifest.sha256>'
LIVE_ENV_SHA='<prepare.templates.env.sha256>'
```

在确认 owner 已批准维护窗口后，可将三步合并为一个有边界的事务入口：

```bash
"$PY" scripts/pnc_rca_minimal_release.py activate \
  --release-note "$RELEASE_NOTE" \
  --release-id "$RELEASE_ID" --epoch-id "$EPOCH_ID" \
  --operator "$OPERATOR" --reason "$REASON" \
  --canary-batch-id "$CANARY_BATCH_ID" --canary-issue-id "$CANARY_ISSUE_ID" \
  --canary-state-path "$CANARY_STATE" \
  --host-branch "$HOST_BRANCH" --host-tag "$HOST_TAG" \
  --host-runtime-root "$HOST_RUNTIME_ROOT" \
  --worker-remote "$WORKER_GITLAB_REMOTE" --worker-branch "$WORKER_BRANCH" \
  --worker-tag "$WORKER_TAG" --worker-runtime-root "$WORKER_RUNTIME_ROOT" \
  --pipeline-remote "$PIPELINE_GITLAB_REMOTE" --pipeline-branch "$PIPELINE_BRANCH" \
  --pipeline-tag "$PIPELINE_TAG" --pipeline-runtime-root "$PIPELINE_RUNTIME_ROOT" \
  --report-manifest-path /home/mini/.config/g1q3-rca/report-runtime-manifest.json \
  --control-db "$CONTROL_DB" \
  --manifest-output "$MANIFEST_SOURCE" --env-output "$ENV_SOURCE" \
  --confirm-release-id "$RELEASE_ID" \
  --receipt "$APPLY_RECEIPT" --quiesce-receipt "$QUIESCE_RECEIPT"
```

`activate` 不接受预先拼好的 candidate hash；它从同一份 `prepare` 输出推导 plan 输入。`prepare-preflight` 仍可单独运行，但它本身不会停 resident，也不能替代 `activate` 的 bounded window。

## 4. Plan

先执行只读 plan：

```bash
"$PY" scripts/pnc_rca_minimal_release.py plan \
  --release-note "$RELEASE_NOTE" \
  --manifest-source "$MANIFEST_SOURCE" \
  --env-source "$ENV_SOURCE" \
  --expected-manifest-sha256 "$LIVE_MANIFEST_SHA" \
  --expected-env-sha256 "$LIVE_ENV_SHA"
```

Plan 必须同时证明：

- GitLab branch 与 tag 解析到 note 固定的 commit/tag object；
- commit/tree 与 immutable Host runtime 完全一致且 tree clean；
- worker、pipeline、report service identity 完整；
- manifest/env 字节与 note projection 一致；
- current v14 predecessor、transition audit、零 inflight 和 partition fence 精确匹配 note 中的 v14-to-v15 epoch contract；
- resident 当前状态可读。

任一项不一致时修正源输入或 release note 后重新 plan，不修改 production runtime 来迎合 note。

## 5. Apply 与 Resident 读回

Apply 是生产变更，只能由明确操作者在 plan 输入未变化后执行：

```bash
"$PY" scripts/pnc_rca_minimal_release.py apply \
  --release-note "$RELEASE_NOTE" \
  --manifest-source "$MANIFEST_SOURCE" \
  --env-source "$ENV_SOURCE" \
  --expected-manifest-sha256 "$LIVE_MANIFEST_SHA" \
  --expected-env-sha256 "$LIVE_ENV_SHA" \
  --confirm-release-id "$RELEASE_ID" \
  --receipt "$APPLY_RECEIPT"
```

Apply 在同一个 release lock 下执行完整切换：先以 `activation_outcome=unknown` 写并 fsync `started` receipt，再停止六个 resident；持久 enable 四个 required resident、disable Kafka consumer 与 completion relay并读回；随后以 exact expected hash 安装 live manifest/env，并在一个 SQLite `BEGIN IMMEDIATE` 事务内校验 v14 predecessor/audit/inflight/fence、重建 v15 activation schema、激活唯一 steady successor，最后才 CAS schema marker。迁移 outcome 会立即 checkpoint 到同一个 receipt inode；只有独立探针证明 `committed` 后才启动四个 required resident。最后再次读回 GitLab/runtime/live projection/v15 epoch/PID/cwd/entrypoint/health/release binding，原 inode 收敛为 completed receipt。

失败语义按迁移 outcome 分层：`not_committed` 才允许恢复 manifest/env；`committed` 必须保留 v15 artifact、保持六面全停并走 successor-read-only 或 forward fix；`unknown` 同样保持 artifact 和六面全停，必须人工裁决，禁止猜测并回滚 SQLite。任何路径都不伪造 epoch 回滚。迁移提交后，旧 v14 writer binary 不再是自动回滚目标。

`operator_issue_only_v1` 要求运行：

- `ai.hermes.gateway`
- `local.pnc.rca-outbox-dispatcher`
- `local.pnc.rca-delivery-collector`
- `local.pnc.rca-delivery-dispatcher`

要求未加载：

- `local.pnc.rca-kafka-consumer`
- `local.pnc.completion-notice-relay`

不允许用已有 PID、陈旧 health JSON 或文件指纹代替 restart/readback。

## 6. 唯一 Transport Canary

Apply 不自动创建业务任务。每个 release 单独提交一个真实 issue canary，batch state 必须只包含该 issue，并显式选择 transport 轴：

```bash
"$PY" scripts/pnc_rca_batch_rerun.py \
  --control-db "$CONTROL_DB" \
  --queue "$CANARY_QUEUE" \
  --state "$CANARY_STATE" \
  --batch-id "$CANARY_BATCH_ID" \
  --expected-runtime-commit "$HOST_COMMIT" \
  --expected-runtime-tree "$HOST_TREE" \
  --owner-receipt "$CANARY_OWNER_RECEIPT" \
  --acceptance-axis transport
```

Canary 只有在以下条件全部满足时通过：

- state 为 `completed`，唯一 item 为 `accepted` 或 `completed`；
- approval 与 state 都记录 `acceptance_axis=transport`；
- `acceptance.transport.status=pass`；
- transport 记录非空 `official_comment_id`；
- `official_field_keys` 精确为 `field_8c912e` 和 `field_9193cb`；
- official readback source 为 `read_after_write` 或 `read_after_recovery_write`；
- state runtime commit/tree 与本 release Host identity 一致。

`causal_attribution` 同时保留并展示，但不是 Host transport release 的阻断项。不得为了发布验收改写或放宽归因判定。

## 7. Verify 与 GA 判定

Canary 完成后执行：

```bash
"$PY" scripts/pnc_rca_minimal_release.py verify \
  --release-note "$RELEASE_NOTE" \
  --apply-receipt "$APPLY_RECEIPT"
```

只有 `verify` 返回 `ok=true`，且 apply receipt、canary state 与 live readback 都绑定同一 release identity，才可标记 GA。以下证据不能单独证明 GA：

- 单测通过；
- report 文件存在；
- shared-state task completed；
- collector `healthy=true`；
- 本地 delivery receipt 但没有官方字段读回；
- 因果归因 PASS 但 transport 未完成。

## 8. 日常只读检查

```bash
curl -fsS http://127.0.0.1:18789/health/detailed | python3 -m json.tool

for name in \
  outbox_dispatcher_health \
  delivery_collector_health \
  delivery_dispatcher_health; do
  python3 -m json.tool \
    "$HOME/.hermes/runtime/pnc_agent/feishu_issue_kafka_rca/$name.json"
done

~/.local/bin/ssh-mini-status
~/.local/bin/ssh-mini-resource --summary
```

检查时必须把进程存在、PID/cwd、heartbeat 新鲜度、release block 和业务 readiness 分开看。任何一个绿色字段都不能覆盖其他轴的失败。

## 9. Resident Liveness 与旧 Watchdog 退役

RCA resident 的生存和版本证据只由 `pnc_rca_minimal_release.py` 的 readback 判定：

- `launchctl` 必须返回唯一正 PID；
- 实际进程的 cwd、entrypoint、create time 必须匹配 immutable Host runtime；
- health 中的 PID、runtime identity、release block 必须与进程及 release note 一致；
- 三个带 schema 的 RCA health 必须在 120 秒 freshness 窗口内，状态不得为 starting、stopped、disabled、error 或 circuit open；Gateway 没有连续 heartbeat，常规 `verify` 以 live PID/cwd/entrypoint 和 `gateway_state=running` 为准，apply 的 restart transition 仍受 120 秒窗口约束；
- Kafka consumer 与 completion relay 必须保持未加载；
- `verify` 在同一 release lock 内做两次 resident identity 读回，期间 PID 或 identity 变化即失败。

旧 `local.pnc.watcher-staleness-watchdog` 通过 launchd plist 文本和进程命令行猜测加载关系，既不能可靠识别 `pnc_live_exec.py` exec 后的进程，也没有把 `actual=down` 纳入告警，因此不再作为 RCA liveness 或发布门禁。仓库不再分发它的 plist 和 shell 脚本。

`hermes-release-fingerprint-check` 暂时保留为通用、手工调用的非 RCA binding 审计工具；它的 `--watchers-fresh` 结果不得用于 RCA 验收。删除该通用 CLI 前必须先迁移 `context_local_source_retirement.py` 和 `context_local_rebuildable_retirement.py` 的 strict binding 消费链。

生产机上的历史 label、plist 和 `runtime/governance-tools/watcher-staleness-watchdog.sh` 不随候选代码删除。只有在新 release 的 completed apply receipt、resident readback 和 transport canary 全部通过后，才能在单独的生产审批中执行 `launchctl bootout` 和 live 文件退役；随后至少跨过两个旧 `StartInterval` 窗口，确认旧日志不再增长，并再次运行 minimal `verify`。候选代码审查、测试或 GitLab push 均不得执行这些 live 动作。

## 10. 失败恢复与回滚上限

先读取 apply receipt 的 `activation_outcome` 和 `rollback_ceiling`，不得只根据 CLI 退出码判断：

1. `not_committed / artifact_restore_permitted`：确认独立 outcome probe 仍证明完整 v14 preimage 后，才允许恢复本次 manifest/env preimage。
2. `committed / successor_read_only_or_forward_fix`：保留 v15 DB 和 candidate artifact，保持六个 resident 全停；修复 v15 binary 后执行 forward apply 和完整 readback，禁止启动 v14 writer。
3. `unknown / operator_adjudication_required`：保持六个 resident 全停且不替换 artifact，由操作者使用同一 note 的只读 outcome probe 裁决；不得把 unknown 当作 not committed。

当前 driver 不提供 v15-to-v14 schema 回滚，也不允许用同一 note 创建另一个 v15 epoch。未来 v15 release 或回滚必须使用另行审定的 v15-aware 合同。禁止原地修改 runtime、手工拼 binding、复活旧 epoch、直接改 SQLite 或绕过 canary。历史任务和 delivery effect 保持不可变；确需业务重试时创建新 generation。

## 11. 定向验证

代码变更在 clean GitLab worktree 中运行最小相关测试；不要进入 production runtime 改代码或跑会生成文件的开发测试。Host 发布工具至少覆盖：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -q -o addopts='' \
  tests/scripts/test_pnc_rca_minimal_release.py \
  tests/scripts/test_pnc_rca_batch_rerun.py
```

VM 业务仓、归因 evaluator 和报告语义由各自负责人测试与发布；Host release 验收只读取它们的 immutable identity，不建立平行实现。
