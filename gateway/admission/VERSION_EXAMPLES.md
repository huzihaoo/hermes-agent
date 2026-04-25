# Admission 版本管理 — 使用示例

## 场景 1：修复一个 Bug（PATCH 升级）

假设发现 `queue.py` 中 `_pop_round_robin` 有个边界条件 bug。

### 步骤

```bash
cd /Users/songying/.hermes/hermes-agent

# 1. 查看当前版本
python -c "from gateway.admission import __version__; print(__version__)"
# 输出: 1.0.1

# 2. 修复 bug
vim gateway/admission/queue.py
# 在文件头部添加版本标记：
# Changed in: v1.0.2 — fixed _pop_round_robin edge case when all items in backoff

# 3. 跑测试
source venv/bin/activate
pytest tests/gateway/test_admission_*.py -q
# 确认: XX passed, 0 failed

# 4. 更新版本号
vim gateway/admission/__init__.py
# 修改: __version__ = "1.0.2"
# 在 Version History 中追加:
#   v1.0.2 (2026-04-26) — Bug fixes
#     - Fixed _pop_round_robin edge case when all items in backoff

# 5. 更新 CHANGELOG
vim gateway/admission/CHANGELOG.md
# 在顶部追加:
## [1.0.2] — 2026-04-26 — Bug fixes

### Fixed
- **`_pop_round_robin` edge case**: 当所有 items 都在 backoff 时返回 None 而非死循环

# 6. 运行版本检查
python gateway/admission/version_check.py
# 确认: ✅ Version 1.0.2 is consistent and ready to commit

# 7. 提交
git add gateway/admission/
git commit -m "fix(admission): v1.0.2 — _pop_round_robin edge case

Fixed: _pop_round_robin now correctly returns None when all items
are in backoff, instead of infinite loop.

Tests: 62/62 passed"
```

---

## 场景 2：新增功能（MINOR 升级）

假设要新增一个 `"urgent"` lane，优先级高于 `"fast"`。

### 步骤

```bash
# 1. 当前版本: 1.0.2 → 升级到 1.1.0（新功能）

# 2. 修改代码
vim gateway/admission/types.py
# 修改: ALL_LANES = ("urgent", "fast", "standard", "heavy")
# 添加版本标记: Changed in: v1.1.0 — added "urgent" lane

vim gateway/admission/controller.py
# 更新 _classify_lane() 逻辑
# 添加版本标记: Changed in: v1.1.0 — added "urgent" lane classification

# 3. 写测试
vim tests/gateway/test_admission_urgent_lane.py
# 新增测试文件

# 4. 跑测试
pytest tests/gateway/test_admission_*.py -q
# 确认: XX passed, 0 failed

# 5. 更新版本号
vim gateway/admission/__init__.py
# 修改: __version__ = "1.1.0"
# 在 Version History 中追加:
#   v1.1.0 (2026-05-01) — New features
#     - Added "urgent" lane for critical messages
#     - Updated lane classification logic

# 6. 更新 CHANGELOG
vim gateway/admission/CHANGELOG.md
## [1.1.0] — 2026-05-01 — New features

### Added
- **"urgent" lane**: 新增紧急车道，优先级高于 fast
- **Lane 分类逻辑更新**: 包含 "紧急" / "urgent" 关键词的消息自动进入 urgent lane

### Changed
- `ALL_LANES` 现在是 `("urgent", "fast", "standard", "heavy")`

# 7. 运行版本检查
python gateway/admission/version_check.py

# 8. 提交
git add gateway/admission/ tests/gateway/test_admission_urgent_lane.py
git commit -m "feat(admission): v1.1.0 — added urgent lane

New feature: urgent lane for critical messages with highest priority.

Changes:
- types.py: ALL_LANES now includes 'urgent'
- controller.py: _classify_lane() detects urgent keywords
- New test: test_admission_urgent_lane.py

Tests: XX/XX passed"
```

---

## 场景 3：破坏性变更（MAJOR 升级）

假设要将 `domain_id` 从字符串改为结构化对象（破坏持久化格式）。

### 步骤

```bash
# 1. 当前版本: 1.1.0 → 升级到 2.0.0（破坏性变更）

# 2. 实施变更 + 数据迁移脚本
vim gateway/admission/types.py
# 修改 QueueItem.domain_id 类型
# 添加版本标记: Changed in: v2.0.0 — domain_id now structured object

vim gateway/admission/persistence.py
# 添加迁移逻辑: _migrate_v1_to_v2()
# 添加版本标记: Changed in: v2.0.0 — added v1→v2 migration

# 3. 测试（含迁移测试）
pytest tests/gateway/test_admission_*.py -q
pytest tests/gateway/test_admission_migration_v1_v2.py -v

# 4. 更新版本号
vim gateway/admission/__init__.py
# 修改: __version__ = "2.0.0"
# 在 Version History 中追加:
#   v2.0.0 (2026-06-01) — Breaking changes
#     - ⚠️ domain_id now structured object (requires migration)
#     - Automatic v1→v2 migration on first load

# 5. 更新 CHANGELOG
vim gateway/admission/CHANGELOG.md
## [2.0.0] — 2026-06-01 — Breaking changes

### ⚠️ BREAKING CHANGES
- **domain_id 结构化**: 从 `str` 改为 `DomainId` 对象
- **持久化格式变更**: SQLite schema 升级
- **自动迁移**: 首次加载时自动从 v1 迁移到 v2

### Migration Guide
1. 备份现有数据库: `cp ~/.hermes/admission/queue.db queue.db.backup`
2. 升级到 v2.0.0
3. 首次启动时自动迁移
4. 验证: `python -m gateway.admission.cli status`

# 6. 运行版本检查
python gateway/admission/version_check.py

# 7. 提交
git add gateway/admission/ tests/gateway/test_admission_migration_v1_v2.py
git commit -m "feat(admission): v2.0.0 — structured domain_id (BREAKING)

⚠️ BREAKING CHANGE: domain_id is now a structured object.

Changes:
- types.py: DomainId dataclass replaces str
- persistence.py: automatic v1→v2 migration
- All tests updated for new schema

Migration: automatic on first load, backup recommended.

Tests: XX/XX passed"
```

---

## 快速命令参考

```bash
# 查看当前版本
python -c "from gateway.admission import __version__; print(__version__)"

# 查看版本历史
cat gateway/admission/CHANGELOG.md

# 查看哪些文件在哪个版本被改过
grep -r "Changed in: v" gateway/admission/*.py

# 运行版本一致性检查
python gateway/admission/version_check.py

# 跑完整测试套件
pytest tests/gateway/test_admission_*.py -q

# Git 历史
git log --oneline -- gateway/admission/
```
