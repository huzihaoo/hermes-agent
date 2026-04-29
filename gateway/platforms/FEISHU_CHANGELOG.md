# Feishu Gateway Capabilities — CHANGELOG

所有变更按 **gstack 增量版本管控** 规范记录。该文件跟踪 Feishu/Lark gateway 用户可见能力，不替代 Hermes Agent 主版本或 Admission 版本。

## 版本号规则

```
MAJOR.MINOR.PATCH
  │     │     └─ bug fix / fail-closed 修复 / 文案兼容（不改用户流程）
  │     └─ 新入口 / 新消息类型 / 新用户可见能力
  └─ 破坏性变更（权限模型、消息格式、路由契约不兼容）
```

---

## [1.2.0] — 2026-04-29 — 话题数据文件入口与用户可见降级

### Added
- Feishu 话题内普通小文件可被机器人读取、缓存，并作为任务上下文交给 agent 使用。
- 新增 host 文件下载上限策略：`HERMES_FEISHU_MAX_FILE_BYTES`，默认 32MB。
- `HERMES_FEISHU_MAX_FILE_BYTES=0` 作为附件下载 kill switch，调用 Feishu SDK 下载前即短路。
- folder / oversized / Feishu 下载限制场景返回用户可见提示，而不是静默表现为“没看到文件”。
- 大文件交接提示统一指向 VM `/mnt/tmp/<task_id>/...` 和用户可见 CIFS 路径。

### Changed
- warning-only 附件事件会降级为 text message，确保 agent 和用户都能看到处理建议。
- Drive folder traversal 不在 v1 范围内；folder 消息明确提示使用 zip 或 VM/NAS 路径。

### Fixed
- 避免 `HERMES_FEISHU_MAX_FILE_BYTES=0` 时仍先调用 `message_resource.get()` 导致 SDK 可能提前下载/缓冲。
- Feishu 资源下载错误码（如 234037）转为可理解的用户提示。

### Verified
- `tests/gateway/test_feishu.py tests/gateway/test_run_document_preprocessing.py tests/tools/test_send_message_tool.py` — 196 passed, 3 warnings
- `py_compile` — gateway Feishu runtime and related tests compiled
- post-restart smoke — zero-cap 不调用 SDK get，返回 `/mnt/tmp` + CIFS handoff warning

---

## [1.1.0] — 2026-04-29 — 话题回复 fallback 与 VM 路径契约

### Added
- Feishu topic 发送目标支持将 raw `om_...` anchor 归一化为 topic metadata。
- VM artifact path response contract：用户可见路径必须包含 CIFS，VM 内部路径使用 `/mnt/tmp/<task_id>/...`。

### Fixed
- 非 internal `2200` reply error 增加回归覆盖。
- reply fallback 避免因 topic/reply 兼容问题导致结果无法发出。

### Verified
- Feishu send-message/topic routing 相关回归测试通过。

---

## [1.0.0] — 2026-04-28 — Feishu 任务话题反馈路由闭环

### Added
- Admission / Feishu queue 路由链路保存原始 request message id 与 topic/thread 元数据。
- VM/shared-state feedback 可恢复原始 Feishu 话题路由。

### Fixed
- per-task routing 字段不完整时 fail closed，避免回退到主群或错误话题。

### Verified
- Admission routing / VM task session routing / shared-state host tests passed in release v1.9.0 scope.
