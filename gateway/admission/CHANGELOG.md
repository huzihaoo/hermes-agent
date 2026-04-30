# Admission Control — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。每次大范围改动必须升版本号。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / 并发修复 / 小优化（不改 API）
  │     └─ 新功能 / 新 lane / 新 domain / 新配置项
  └─ 破坏性变更（数据格式迁移、API 签名变化）
```

---

## [1.10.1] — 2026-04-30 — Explicit worker slot accounting

### Fixed
- QueueWorker now uses explicit public slot accounting instead of relying on private semaphore internals, so same-domain serial and different-domain parallel dispatch stay correct under load.
- Invalid `max_concurrent_per_domain` values now fail closed for non-positive integers, floats, and booleans before worker startup.
- Registry-handler execution now forwards scheduler metadata consistently, matching the direct Python tool path.

### Verified
- `tests/gateway/test_feishu_admission_routing.py tests/gateway/test_admission_templates.py tests/tools/test_vm_task_tool_session_routing.py tests/tools/test_vm_task_tool.py tests/gateway/test_admission_worker.py tests/gateway/test_admission_concurrent.py tests/gateway/test_feishu.py tests/tools/test_send_message_tool.py` — 242 passed, 3 warnings
- `tests/gateway/test_feishu_admission_autostart.py tests/gateway/test_admission_templates.py` — 21 passed
- `tests/gateway/test_feishu_admission_routing.py tests/gateway/test_admission_templates.py tests/tools/test_vm_task_tool_session_routing.py tests/tools/test_vm_task_tool.py tests/gateway/test_admission_worker.py tests/gateway/test_admission_concurrent.py tests/gateway/test_feishu.py tests/tools/test_send_message_tool.py` — 237 passed, 16 skipped
- `python check_versions.py` — Admission Control v1.10.1, all modules version-managed
- `py_compile` — release-touched runtime/test modules compiled

## [1.10.0] — 2026-04-29 — VM Git 协作放行与 PNC 数据校验代理

### Added
- `validate_data_validity` PNC agent tool is exposed in the Feishu toolset and submits work through the shared-state/VM worker path.
- VM task briefs now use `/mnt/tmp/<task_id>/` working directories and include the user-visible CIFS artifact path.
- Permission-policy regressions cover narrowly-scoped VM worktree git operations, member push approval, and destructive git denial.

### Changed
- Member/senior VM worktree git collaboration is classified separately from generic VM direct execution when the whole `ssh-mini-run` payload is a validated git-only sequence in the caller's own worktree.
- PNC agent smoke checks now include the `validate-data-validity` Python CLI skeleton.
- Feishu send-message topic targets normalize raw `om_...` anchors to topic metadata before sending.
- Fallback context-length selection now honors configured custom provider limits during model fallback compression.

### Fixed
- VM git collaboration no longer gets over-blocked as arbitrary direct VM execution while force push, `git clean`, `git reset --hard`, unsafe branch deletion, path traversal, and command-smuggling remain fail-closed.
- Feishu approval-card callbacks accept both SDK-native dict values and stringified JSON action values.

### Verified
- `tests/tools/test_permission_policy.py tests/tools/test_pnc_agent_tools.py tests/test_toolsets.py tests/tools/test_send_message_tool.py tests/gateway/test_feishu_approval_buttons.py` — 153 passed
- `tests/gateway/test_admission*.py tests/gateway/test_feishu_admission_autostart.py` — 147 passed
- `tests/gateway/test_feishu.py tests/gateway/test_pairing.py tests/e2e/test_platform_commands.py` — 222 passed
- `python check_versions.py` — Admission Control v1.10.0, all modules version-managed
- `py_compile` — release-touched runtime/test modules compiled

## [1.9.0] — 2026-04-28 — Feishu 任务话题反馈路由闭环

### Added
- Admission queue now persists the original Feishu `request_message_id` alongside chat/thread routing metadata.
- New regression coverage for queued Feishu events preserving `thread_id` and original request message IDs.

### Changed
- Feishu queue reconstruction now uses the original request message ID when replaying admitted messages, keeping topic/thread replies anchored to the source task message.
- `AdmissionController.admit()` accepts `request_message_id` for platform adapters and legacy Feishu bridge callers.
- Queue persistence schema migrates existing SQLite DBs with `request_message_id TEXT`.

### Fixed
- VM/shared-state feedback chain can now preserve original Feishu topic routing via task metadata and host relay restoration.
- Feishu-origin VM notifications fail closed when required per-task routing fields are incomplete instead of falling back to ambient main-chat environment.

### Verified
- `tests/gateway/test_admission_connpool.py tests/gateway/test_queue_persistence.py tests/gateway/test_admission_controller.py tests/gateway/test_feishu_admission_routing.py tests/gateway/test_feishu_admission_autostart.py tests/gateway/test_run_progress_topics.py` — 52 passed
- `tests/tools/test_vm_task_tool_session_routing.py tests/tools/test_vm_task_tool.py tests/gateway/test_feishu_admission_routing.py` — 14 passed
- `workspace-work/tests/test_shared_state_v2_host.py` — 59 passed, 5 subtests passed

---

## [1.8.0] — 2026-04-27 — VM 仓库多用户并发隔离

### Added
- **worktree_audit.py**：worktree 操作审计模块
  - `log_worktree_event(user, repo, action, ...)` — 记录 user → worktree → branch → git 操作
  - `query_user_activity(user, days, repo)` — 查询用户近 N 天的 worktree 操作记录
  - 审计日志写入 `~/.hermes/audit/worktree/YYYY-MM-DD.jsonl`
  - 覆盖主仓库和嵌套仓库（msg/data_proto, tools/*）的 git 操作

- **worktree_manager.py**：worktree 生命周期管理 CLI
  - `ensure <user> <repo> [--branch]` — 自动创建 worktree（不存在时）
  - `list [--user]` — 列出所有 worktree 及分支状态
  - `gc [--older-than N]` — 列出 N 天未访问的 stale worktree
  - `status <user> <repo>` — 查询 worktree 状态（分支、dirty、未提交文件数）
  - 读取 `user-roles.json` 的 `repo_config` 段获取仓库配置

- **VM 侧运维脚本**（部署到 `/home/mini/worktrees/`）
  - `audit-logger.sh` — 轻量审计日志记录（shell 层）
  - `safe-push.sh` — 并发 push 安全包装器（文件锁 + fetch + rebase）
  - `gc-worktrees.sh` — worktree GC（30 天未访问自动清理）
  - `check-disk-quota.sh` — 磁盘配额检查（10GB/用户/仓库）
  - `rotate-audit-log.sh` — 审计日志轮转（10MB 轮转，保留 90 天）

### Changed
- **user-roles.json**：新增 `repo_config` 段（仓库源路径、默认分支、worktree_base）
- **user-roles.json**：补全王中坤角色映射
- **config.yaml system_prompt**：注入完整的 worktree 路由、审计、合并、push 安全、嵌套仓库规则
- **AGENTS.md**：同步 VM 仓库访问规则和合并流程

### VM 侧部署
- 4 仓库 × 4 用户 = 16 个 worktree 已创建
- 5 个嵌套仓库通过 symlink 链接到所有 worktree
  - `msg/data_proto`、`tools/mcap_data_translate`、`tools/data_preprocess`
  - `tools/quality-gate-keeper`、`tools/simulator_with_dnp`

### Verified
- VM 端到端测试 10/10 通过
- 真实场景验证 4/4 通过（新用户 worktree 自动创建、嵌套仓库审计、配额检查、GC dry-run）
- gstack Boil the Lake + QCon Beijing 2026-04 交叉审视完成
- 生产就绪度：8.5/10

### Documentation
- 设计文档：`knowledge/wiki/designs/vm-repo-isolation.md`（v1.0.0 最终版）
- gstack 审视报告：`knowledge/wiki/reviews/vm-repo-isolation-gstack-review.md`
- 快速参考卡片：`knowledge/wiki/quick-refs/vm-repo-access.md`

---

## [1.7.0] — 2026-04-26 — Gateway 启动防护：杜绝进程堆积

### Added
- **全局进程扫描**：`gateway/status.py` 新增 `get_all_gateway_processes()` 和 `_is_process_alive()`
  - 启动时扫描所有 gateway 进程（不只是 PID 文件记录的那个）
  - 提取 HERMES_HOME 环境变量，只清理同一个 HOME 下的进程
  - 兼容多种启动方式：`hermes gateway run`、`python -m hermes_cli.main gateway`、`gateway/run.py`

- **启动防护逻辑**：`gateway/run.py` 的 `start_gateway()` 重构
  - 检测到多个进程时：
    - 不带 `--replace`：拒绝启动，提示用户清理
    - 带 `--replace`：自动清理所有旧进程，然后启动新进程
  - 单个进程时：保持原有逻辑（向后兼容）
  - 无进程时：正常启动

### Fixed
- **进程堆积问题**：从设计上杜绝多个 gateway 进程同时运行
  - 手动启动多次 `hermes gateway run --replace` 不再堆积
  - LaunchAgent 自动重启与手动启动的竞态不再产生多进程
  - 旧进程清理慢时，新进程会等待或强制 kill

### Changed
- **PID 文件不再是唯一真相源**：启动时优先信任全局进程扫描，PID 文件作为辅助
- **--replace 语义增强**：从"替换 PID 文件里的进程"变为"清理所有同 HERMES_HOME 的进程"

### Verified
- 手动测试：单进程启动、多进程拒绝、--replace 自动清理、LaunchAgent 重启
- 兼容性：不影响现有 `hermes gateway start/stop/restart` 命令

### Documentation
- 新增 `DESIGN_GATEWAY_STARTUP_GUARD.md`：设计方案、风险评估、测试用例
- 更新 `RUNBOOK_GATEWAY_PROCESS_PILEUP.md`：补充 v1.7.0+ 的设计防护说明

---

## [1.6.1] — 2026-04-26 — Gateway 进程堆积问题 Runbook

### Added
- **RUNBOOK_GATEWAY_PROCESS_PILEUP.md**：Gateway 进程堆积问题的诊断、解决、预防完整指南
  - 根因分析：PID 文件机制局限、LaunchAgent 竞态、手动启动重试
  - 诊断步骤：进程列表、PID 文件、LaunchAgent 状态
  - 三种解决方案：官方命令、手动清理、保留 CLI 会话
  - 预防措施：避免手动启动、监控进程数量、日志审计
  - 常见问题 FAQ

### Context
- 今日遇到多个 `hermes gateway run --replace` 进程堆积，导致新 gateway 启动失败
- 根因：`--replace` 只能 kill PID 文件记录的进程，旧进程清理慢时新进程已启动
- 解决：`hermes gateway stop` + `hermes gateway start` 清理所有进程

---

## [1.6.0] — 2026-04-26 — FeishuAdapter 自动集成

### Added
- **FeishuAdapter 自动启停 MetricsServer**：读取 `config.extra.admission_metrics_port`，在 `connect()` 时启动，`disconnect()` 时停止
- **FeishuAdapter 自动加载 template**：读取 `config.extra.admission_template`，在 admission 初始化后自动 apply
- **配置示例补充**：README 新增 `admission_metrics_port` 和 `admission_template` 配置说明

### Verified
- 新增 `test_feishu_admission_autostart.py`（8 tests）：template 自动加载、metrics server 启停、组合场景
- 修复 `test_admission_autostart.py` 中的默认值断言（rate_limit 默认 20）
- admission 全量测试 139/139 通过

---

## [1.5.0] — 2026-04-26 — 运维面板闭环

### Added
- **README 全面更新**：版本号 → v1.5.0，文件结构、告警/模板/Prometheus/CLI 全部补齐
- **CLI `alerts` 子命令**：查看告警历史 (`python -m gateway.admission.cli alerts`)
- **CLI `apply` 子命令**：从 store 加载模板并应用 (`python -m gateway.admission.cli apply strict`)
- **MetricsServer**：独立 HTTP server 暴露 `/metrics` 端点（stdlib http.server，零侵入 gateway）

### Verified
- 新增 `test_admission_cli_ext.py`（4 tests）+ `test_admission_metrics_server.py`（3 tests）
- admission 全量测试 135/135 通过

---

## [1.4.0] — 2026-04-26 — Prometheus metrics exporter

### Added
- **MetricsExporter**：新增 `metrics_export.py`，纯字符串生成 Prometheus exposition format，无外部依赖
- 导出 counter：admitted / rejected / completed / failed / retried / dead
- 导出 gauge：`admission_queue_depth{domain,lane}` 每个 domain×lane 组合
- 导出 counter：`admission_alerts_fired_total`

### Verified
- 新增 `test_admission_metrics_export.py`（5 tests）
- admission 全量测试 128/128 通过

---

## [1.3.0] — 2026-04-26 — 边界测试 + Shutdown 压力 + Connection Pool

### Added
- **边界测试**：空队列全 lane/domain dequeue、100 domain_id 极端数量、1000 item 单 lane 满载、backoff 全阻塞、cleanup 边界
- **Graceful shutdown 压力测试**：drain 内完成、超时取消、空队列快速停止、double stop、10 并发 in-flight drain
- **ConnectionPool**：`persistence.py` 新增 `ConnectionPool` 类，模块级 pool 缓存，所有 save/load 复用同一连接，不再每次 open/close

### Verified
- 新增 `test_admission_boundary.py`（18 tests）+ `test_admission_shutdown.py`（5 tests）+ `test_admission_connpool.py`（5 tests）
- admission 全量测试 123/123 通过

---

## [1.2.0] — 2026-04-26 — 策略模板

### Added
- **PolicyTemplate**：可复用的准入策略配置数据类（rate limit / depth threshold / error rate）
- **TemplateStore**：JSON 文件持久化，支持 CRUD / import / export / seed
- **Built-in templates**：`strict`（严格）/ `relaxed`（宽松）/ `vip-priority`（VIP 优先）
- **AdmissionController.apply_template()**：运行时热切换策略模板
- **CLI `template` 子命令**：`list` / `seed` / `export` / `import`

### Verified
- 新增 `tests/gateway/test_admission_templates.py` + `tests/gateway/test_admission_cli.py`
- admission 全量测试 95/95 通过

---

## [1.1.0] — 2026-04-26 — 告警规则

### Added
- **Alert framework**：新增 `alerts.py`，包含 `AlertManager`、`AlertRecord`、`AlertLevel`
- **Queue depth alert**：支持 warning / critical 双阈值告警
- **Error rate alert**：基于 `total_completed + total_failed + total_dead` 计算错误率并告警
- **Cooldown suppression**：同一规则支持冷却期，避免重复刷屏
- **Alert callbacks + history**：支持回调投递与历史查询
- **Controller integration**：`AdmissionController` 内置告警管理，队列深度检查改走统一 alert pipeline

### Verified
- 新增 `tests/gateway/test_admission_alerts.py`
- 17/17 告警测试通过
- admission 全量测试 79/79 通过

---

## [1.0.1] — 2026-04-25 — 并发硬化

### Fixed
- **`_gc_empty_domain_id` 竞态**：`defaultdict` 删除后并发 enqueue 重建 entry，用 `pop(key, None)` 防 KeyError
- **`_metrics` 非原子递增**：新增 `_metrics_lock`，所有 `+=` 操作加锁
- **persistence DELETE 全表重写**：改用 `INSERT OR REPLACE` upsert，消除并发写丢失

### Verified
- 62/62 测试通过（含 3 个并发压力测试）
- `test_concurrent_enqueue_no_data_loss`：10 线程 × 50 items = 500，0 丢失
- `test_concurrent_dequeue_no_duplicate_items`：5 worker 并发出队，0 重复
- `test_concurrent_enqueue_dequeue_mixed`：5 producer + 3 consumer，100% 一致性

---

## [1.0.0] — 2026-04-24 — 初始发布

### Added
- **三层隔离**：domain → domain_id → lane
- **三条车道**：fast（短消息）/ standard（标准）/ heavy（编码/部署）
- **三个域**：user（私聊）/ group（群聊）/ vm（虚拟机）
- **Round-robin 调度**：domain_id 间公平轮转
- **优先级**：owner(100) > admin(50) > senior(30) > member(10)
- **限流**：滑动窗口 per-user rate limit
- **重试**：指数退避 + 死信队列
- **持久化**：SQLite + WAL 模式
- **审计**：JSON Lines 审计日志
- **异步 Worker**：per-domain dispatcher + semaphore 并发控制
- **飞书集成**：`FeishuAdmissionBridge` 接入 gateway adapter
- **CLI**：`/queue` 状态查看 + `/stats` 统计

### Files
```
gateway/admission/
├── __init__.py          # 模块入口 + 版本号
├── types.py             # Lane/Domain/QueueItem 数据类型
├── queue.py             # 内存队列 + 线程安全
├── controller.py        # 准入控制器（权限、限流、重试、审计）
├── persistence.py       # SQLite 持久化（WAL + upsert）
├── worker.py            # 异步 Worker（per-domain dispatcher）
├── audit.py             # 审计日志
├── feishu_integration.py # 飞书适配器桥接
├── cli.py               # CLI 命令
├── README.md            # 使用文档
└── CHANGELOG.md         # ← 本文件
```

---

## 升版本 Checklist

每次修改 admission 模块前：

1. [ ] 确认当前版本号（`gateway/admission/__init__.py`）
2. [ ] 评估变更范围 → 决定 MAJOR/MINOR/PATCH
3. [ ] 实施变更
4. [ ] 跑完整测试：`pytest tests/gateway/test_admission_*.py -q`
5. [ ] 更新 `__version__` 和本 CHANGELOG
6. [ ] git commit 消息包含版本号：`feat(admission): v1.x.x — 描述`

## 待办（下一版本候选）

- [x] 边界测试：空队列 dequeue、极端 domain_id 数量、单 lane 满载 *(v1.3.0)*
- [x] Worker graceful shutdown 压力测试 *(v1.3.0)*
- [x] 持久化 connection pool（当前每次 save/load 都 open/close）*(v1.3.0)*
- [x] Prometheus metrics exporter *(v1.4.0)*
