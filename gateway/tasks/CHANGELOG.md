# Task Product Layer — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / 小优化（不改 API）
  │     └─ 新功能 / 新类型 / 新命令
  └─ 破坏性变更（数据格式迁移、API 签名变化）
```

---

## [1.4.7] — 2026-06-08 — G1Q3 RCA intake state + completion probe hardening

### Added
- G1Q3 RCA intake now has explicit issue-context, request-artifact, schema, and state-machine building blocks, so Feishu-side intake can hand off a structured execution request instead of implicit free-form context.
- VM task completion probing now has focused release coverage for the completion-readiness path used by long-running G1Q3 RCA work.

### Changed
- Group binding / delivery policy / gateway runtime handoff paths are tightened so RCA status handoff and completion feedback use the same governed slice instead of drifting across ad-hoc task routing behavior.

### Verified
- Focused tests for `test_pnc_group_binding_status_handoff`, `test_pnc_issue_context`, `test_pnc_rca_artifacts`, `test_pnc_rca_schema`, `test_pnc_rca_state_machine`, `test_vm_task_completion_probe`, and selected Feishu polling diagnostics passed on 2026-06-08.

---

## [1.4.6] — 2026-06-04 — G1Q3 RCA Entry + Runtime Feedback Closeout

### Added
- G1Q3 RCA Feishu entry now has a governed group-binding dry-run and monitor path, so long RCA work can enter through a controlled Feishu-facing surface instead of ad-hoc routing.
- Dashboard and task detail surfaces expose clearer collaboration, artifact, verification, and handoff evidence for VM-backed tasks.
- Local Mem0 sidecar setup and doctor checks are wired as an explicit plugin/runtime capability rather than an implicit memory fallback.

### Changed
- Agent runtime fallback/cooldown and error classification paths now report provider/runtime failures more explicitly and avoid repeating obviously failing lanes.
- Gateway busy, shutdown, restart, and long-running task notices are more human-facing and less tool-noisy.
- VM task feedback policy distinguishes short inspection work from long evaluation/replay work, with safer scheduler metadata normalization.

### Verified
- Version/readiness inspection: live gateway reports PNC-Agent v0.14.4 baseline before bump, Feishu/API server health connected, and this release bump advances Hermes Agent to v0.14.5 / Task Product Layer to v1.4.6.
- Focused release verification is recorded in this release flow; external push/production restart/Feishu send remain explicit-boundary actions unless separately approved.

---

## [1.4.5] — 2026-05-26 — Memory Flush Scope Guard

### Changed
- Pre-reset memory flush agents now receive Feishu/gateway session scope metadata, so built-in memory writes during session rotation cannot fall back to global owner memory.
- Session context cleanup now clears the session-search block flag explicitly alongside other per-turn context variables.

### Verified
- `python -m pytest tests/gateway/test_memory_flush_scope_guard.py tests/gateway/test_session_env.py -q -o addopts=` → 13 passed.
- `python -m py_compile gateway/run.py gateway/session_context.py` passed.

---

## [1.4.4] — 2026-05-26 — Release Flow + Memory Scope + Task Evidence

### Added
- Release tooling now has a guarded Feishu target gate, dry-run-first `publish_all` entrypoint, one-shot Feishu upload/readback wrapper, and standard Markdown template generation with automatic VM HTML URL injection.
- Dashboard task views expose stronger observability evidence, including evidence paths, sidecar-only VM task discovery, active filter highlighting, and a detail-page evidence matrix for artifacts, verification, blockers, and missing source layers.
- Feishu topic follow-ups now receive compact current-topic task context from TaskStore, reducing wrong-task answers after session resets or short replies such as “报告为什么没有贴出来？”.

### Changed
- Built-in memory writes now carry provenance metadata across runtime tool paths and auto memory review.
- Non-CLI/gateway scoped durable-memory writes fail closed instead of silently falling back to global owner memory when no safe scoped store exists.
- Release closeout now includes Feishu target and release publisher/template focused regressions.

### Verified
- `git diff --check` passed for release, memory, gateway task-context, and Dashboard slices.
- `uv run --extra dev pytest tests/scripts/test_pnc_release_html_gate.py tests/scripts/test_pnc_release_browser_gate.py tests/scripts/test_pnc_release_feishu_target_common.py tests/scripts/test_pnc_release_feishu_target_gate.py tests/scripts/test_pnc_release_feishu_publish.py tests/scripts/test_pnc_release_feishu_publish_run.py tests/scripts/test_pnc_release_publish_all.py tests/scripts/test_pnc_release_markdown_template.py -q -o addopts=` → 30 passed.
- `uv run --extra dev pytest tests/tools/test_memory_tool_scope_guard.py tests/tools/test_memory_tool_runtime_scope_guard.py -q -o addopts=` → 8 passed.
- `uv run --extra dev pytest tests/gateway/test_feishu_topic_task_context.py tests/hermes_cli/test_task_views.py -q -o addopts=` → 21 passed.
- `npm run build` in `web/` passed.
- Browser smoke verified real Dashboard task list filtering and detail-page evidence matrix for `vm-bridge-real-integration-20260526-0848`.

---

## [1.4.3] — 2026-05-26 — Task Commands + User-Facing Hygiene

### Added
- CLI/Gateway command registry now exposes `/task`, `/cancel`, and `/retry-task` as gateway-aware task controls so help/discovery matches the live task product layer.
- Dashboard task lists now include active shared-state-only VM tasks that have not yet been mirrored into the local TaskStore, so the main task page can surface VM worker execution instead of only detail fallback.
- Added a report-only skill quality audit for bundled `SKILL.md` files. It checks trigger clarity, verification sections, pitfalls, oversized bodies, and missing local references without turning advisory findings into a gate by default.
- Added a dry-run skill rule snippet exporter so selected Hermes skills can be previewed for AGENTS.md, CLAUDE.md, or Cursor rule files without hand-copying divergent instructions.
- Added the built-in `coding-behavior-baseline` skill as a small reusable engineering baseline for surgical diffs and verification-first coding.

### Changed
- Feishu/proxy-visible progress text now uses mode-aware sanitization so final user-visible replies avoid leaking internal tool traces.
- Gateway task summaries render real newlines instead of escaped `\\n` sequences in task receipts.
- Default max output token hard cap is reduced to 32768 unless explicitly overridden, avoiding repeated oversized truncated JSON retries on providers that already produced truncated tool-call arguments.
- Several bundled skill descriptions and bodies now include clearer "Use when", pitfalls, and verification guidance for better skill routing.

### Verified
- `git diff --check` passed.
- `uv run --extra dev --extra web pytest tests/hermes_cli/test_task_views.py tests/hermes_cli/test_web_tasks_api.py tests/gateway/test_stream_consumer_fresh_final.py tests/gateway/test_task_browse_gateway.py tests/hermes_cli/test_version_command.py tests/scripts/test_audit_skill_quality.py tests/scripts/test_export_skill_rule_snippets.py -q -o 'addopts='` → 57 passed.
- `python check_versions.py` passed.
- `uv run --extra dev python scripts/audit_skill_quality.py skills/software-development/coding-behavior-baseline --fail-on-findings` passed with 0 findings.
- `uv run --extra dev python scripts/render_skill_rule_snippets.py skills/software-development/coding-behavior-baseline/SKILL.md --format all --max-chars 600` produced snippets for agents, claude, and cursor.
- Release artifact sha checks passed for `dist/hermes-agent/hermes-agent-v0.14.2.tar.gz` and `dist/tasks/tasks-v1.4.3.tar.gz`.

---

## [1.4.2] — 2026-05-24 — Dashboard Task Observability API

### Added
- Dashboard read-only task views can list tasks by status/platform and open task details with sidecar progress, artifacts, verification, and blockers.
- TaskStore list/count filters now support platform filtering for Feishu/CLI separation.

### Changed
- Long-running task views mark stale running tasks after 24 hours without sidecar or completion updates.

### Verified
- `uv run --extra dev --extra web pytest tests/run_agent/test_run_agent.py::TestRunConversation::test_repeated_length_finish_reason_returns_bounded_partial tests/test_sub2api_output_cap_static.py tests/gateway/test_task_store_pagination.py::test_pagination_with_platform_filter tests/gateway/test_task_store_pagination.py::test_pagination_combines_platform_and_status_filters tests/gateway/test_telegram_noise_filter.py::test_feishu_final_response_uses_progress_mode tests/hermes_cli/test_task_views.py tests/hermes_cli/test_web_tasks_api.py -q -o 'addopts='` → 27 passed.

---

## [1.4.0] — 2026-04-30 — Routing Metadata + Delivery Diagnostics

### Added
- Task records now persist `chat_id`, `chat_type`, `thread_id`, `message_id`, `error_class`, `error_message`, `receipt_path`, and `delivery_verified`.
- Older `tasks.db` files migrate additively with allowlisted SQLite columns.

### Changed
- Task receipts now reuse stored error class/message when explicit error values are not supplied.

### Verified
- `tests/gateway/test_tasks_store.py tests/gateway/test_tasks_integration.py tests/gateway/test_task_trace_gateway.py` included in the 277-test release slice.

---

## [1.3.0] — 2026-04-26 — Task Cancel + Retry

### Added
- **TaskStore.cancel_task**：支持取消 `pending` / `running` 状态任务
- **TaskStore.retry_task**：支持将 `failed` / `cancelled` 状态任务重置为 `pending`
- **Gateway 子命令**：`/task cancel <id>`、`/task retry <id>`
- **归属校验**：仅任务所属用户可执行取消/重试

### Verified
- 20/20 cancel+retry tests passing

---

## [1.2.0] — 2026-04-26 — Pagination + Status Filter

### Added
- **TaskStore.list_recent 分页**：新增 offset 和 status 参数
- **TaskStore.count_tasks**：按 user_id / status 统计任务总数
- **/tasks [page]**：飞书群内分页浏览任务列表（10条/页）
- **分页导航**：footer 显示上一页/下一页提示

### Verified
- 9/9 pagination tests passing
- 98/98 total core tests passing

---

## [1.1.0] — 2026-04-26 — Webhook Template Integration

### Added
- **Webhook template_id 支持**：webhook routes 可绑定 template_id
- **Template prompt fallback**：route 无 prompt 时使用模板的 request_summary
- **Template skills fallback**：route 无 skills 时使用模板的 skills
- **Template usage tracking**：webhook 触发时自动记录模板使用次数

### Verified
- 5/5 webhook template integration tests passing

---

## [1.0.0] — 2026-04-25 — 初始发布

### Added
- **Task/TaskReceipt 类型**：统一任务对象与标准回执结构
- **TaskStatus 枚举**：pending / running / completed / failed / cancelled
- **TaskType 枚举**：coding / docs / research / chat / cron / unknown
- **_infer_task_type()**：基于关键词推断任务类型
- **SQLite TaskStore**：upsert / get / list_recent 持久化
- **TemplateStore**：create_from_task / get / list_recent / record_usage
- **EventEmitter 集成**：task:start / task:complete / task:failed 自动同步到 TaskStore
- **Gateway 命令**：
  - `/tasks` — 产品级最近任务列表（类型图标 + 状态图标）
  - `/task <id>` — 完整任务详情页（耗时、工具调用、错误信息）
  - `/template create <task_id> <name>` — 从成功任务创建模板
  - `/templates` — 模板列表
- **Cron 模板绑定**：cron jobs 支持 template_id

### Files
```
gateway/tasks/
├── __init__.py     # 模块入口 + 版本号
├── types.py        # Task/TaskReceipt/TaskStatus/TaskType
├── store.py        # SQLite TaskStore
├── template.py     # TemplateStore
└── CHANGELOG.md    # ← 本文件
```

### Verified
- 36/36 tests passing
- TaskStore: 6 tests
- EventEmitter + TaskStore 集成: 5 tests
- TemplateStore: 4 tests
- Template 命令: 4 tests
- task_trace CLI: 4 tests
- Gateway 命令: 2 tests
- EventEmitter 基础: 11 tests

---

## 升版本 Checklist

1. [ ] 确认当前版本号（`gateway/tasks/__init__.py`）
2. [ ] 评估变更范围 → 决定 MAJOR/MINOR/PATCH
3. [ ] 实施变更
4. [ ] 跑测试：`pytest tests/hermes_cli/test_task_*.py tests/gateway/test_webhook_template_integration.py -q`
5. [ ] 更新 `__version__` 和本 CHANGELOG
6. [ ] git commit 消息包含版本号：`feat(tasks): v1.x.x — 描述`

## 待办（下一版本候选）

- [x] 任务列表分页 (v1.2.0)
- [x] 任务取消功能 (v1.3.0)
- [x] 任务重试功能 (v1.3.0)
- [ ] 模板市场 / 模板分享
- [ ] Dashboard UI 集成
