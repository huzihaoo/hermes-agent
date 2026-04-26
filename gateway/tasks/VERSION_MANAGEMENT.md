# Task Product Layer — Version Management Quick Reference

## 当前版本

```python
from gateway.tasks import __version__
print(__version__)  # "1.1.0"
```

## 查看版本历史

```bash
# 完整 CHANGELOG
cat gateway/tasks/CHANGELOG.md

# Git 历史
git log --oneline -- gateway/tasks/
```

## 升版本流程

### 1. 评估变更范围

| 变更类型 | 版本号变化 | 示例 |
|---------|-----------|------|
| Bug fix / 小优化 | PATCH +1 | 1.1.0 → 1.1.1 |
| 新功能 / 新类型 / 新命令 | MINOR +1 | 1.1.0 → 1.2.0 |
| 破坏性变更 / 数据迁移 | MAJOR +1 | 1.2.0 → 2.0.0 |

### 2. 实施变更

在源文件头部注入版本标记（可选）：
```python
"""...
Changed in: v1.2.0 — 描述
"""
```

### 3. 验证

```bash
cd /Users/songying/.hermes/hermes-agent
source venv/bin/activate

# 跑测试套件
pytest tests/hermes_cli/test_task_*.py \
       tests/gateway/test_webhook_template_integration.py -q

# 确认通过
```

### 4. 更新版本记录

```bash
# 1. 更新 __init__.py
vim gateway/tasks/__init__.py
# 修改 __version__ = "1.2.0"
# 在 docstring 的 Version History 中追加新版本

# 2. 更新 CHANGELOG.md
vim gateway/tasks/CHANGELOG.md
# 在顶部追加新版本段落
```

### 5. Git Commit

```bash
git add gateway/tasks/
git commit -m "feat(tasks): v1.2.0 — 简短描述

详细变更：
- 修复了 X
- 新增了 Y
- 优化了 Z

Tests: XX/XX passed"
```

## 版本号语义

```
1.1.0
│ │ └─ PATCH: 向后兼容的 bug fix
│ └─ MINOR: 向后兼容的新功能
└─ MAJOR: 破坏性变更
```

## 快速检查

```bash
# 当前版本
python3 -c "import sys; sys.path.insert(0, '.'); from gateway.tasks import __version__; print(__version__)"

# 最近 5 次 tasks 相关提交
git log --oneline -5 -- gateway/tasks/

# 测试覆盖率
pytest tests/hermes_cli/test_task_*.py \
       tests/gateway/test_webhook_template_integration.py \
       --cov=gateway.tasks --cov-report=term-missing
```

## 回滚到旧版本

```bash
# 查看历史版本
git log --oneline -- gateway/tasks/__init__.py

# 回滚到特定 commit
git checkout <commit-hash> -- gateway/tasks/

# 验证
pytest tests/hermes_cli/test_task_*.py -q
```

## 版本兼容性矩阵

| tasks 版本 | hermes-agent 版本 | Python | SQLite | 备注 |
|-----------|------------------|--------|--------|------|
| 1.0.0 - 1.1.0 | 0.9.0+ | 3.11+ | 3.35+ | 需要 EventEmitter |
