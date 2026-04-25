# Admission Control — Version Management Quick Reference

## 当前版本

```python
from gateway.admission import __version__
print(__version__)  # "1.0.1"
```

## 查看版本历史

```bash
# 完整 CHANGELOG
cat gateway/admission/CHANGELOG.md

# 哪些文件在哪个版本被改过
grep -r "Changed in: v" gateway/admission/*.py

# Git 历史
git log --oneline -- gateway/admission/
```

## 升版本流程

### 1. 评估变更范围

| 变更类型 | 版本号变化 | 示例 |
|---------|-----------|------|
| Bug fix / 并发修复 / 小优化 | PATCH +1 | 1.0.0 → 1.0.1 |
| 新功能 / 新 lane / 新配置项 | MINOR +1 | 1.0.1 → 1.1.0 |
| 破坏性变更 / 数据迁移 | MAJOR +1 | 1.1.0 → 2.0.0 |

### 2. 实施变更

```bash
# 在源文件头部注入版本标记
# 示例：queue.py
"""...
Changed in: v1.0.1 — _gc_empty_domain_id race fix
"""
```

### 3. 验证

```bash
cd /Users/songying/.hermes/hermes-agent
source venv/bin/activate

# 跑完整测试套件
pytest tests/gateway/test_admission_*.py -q

# 确认通过
# Expected: XX passed, 0 failed
```

### 4. 更新版本记录

```bash
# 1. 更新 __init__.py
vim gateway/admission/__init__.py
# 修改 __version__ = "1.0.2"
# 在 docstring 的 Version History 中追加新版本

# 2. 更新 CHANGELOG.md
vim gateway/admission/CHANGELOG.md
# 在顶部追加新版本段落

# 3. 在修改的源文件头部注入版本标记
# 示例：
# Changed in: v1.0.2 — 描述
```

### 5. Git Commit

```bash
git add gateway/admission/
git commit -m "feat(admission): v1.0.2 — 简短描述

详细变更：
- 修复了 X
- 新增了 Y
- 优化了 Z

Tests: XX/XX passed"
```

## 版本号语义

```
1.0.1
│ │ └─ PATCH: 向后兼容的 bug fix
│ └─ MINOR: 向后兼容的新功能
└─ MAJOR: 破坏性变更
```

## 快速检查

```bash
# 当前版本
python3 -c "from gateway.admission import __version__; print(__version__)"

# 最近 5 次 admission 相关提交
git log --oneline -5 -- gateway/admission/

# 测试覆盖率
pytest tests/gateway/test_admission_*.py --cov=gateway.admission --cov-report=term-missing
```

## 回滚到旧版本

```bash
# 查看历史版本
git log --oneline -- gateway/admission/__init__.py

# 回滚到特定 commit
git checkout <commit-hash> -- gateway/admission/

# 验证
pytest tests/gateway/test_admission_*.py -q
```

## 版本兼容性矩阵

| admission 版本 | hermes-agent 版本 | Python | SQLite | 备注 |
|---------------|------------------|--------|--------|------|
| 1.0.0 - 1.0.1 | 0.9.0+ | 3.11+ | 3.35+ | 需要 WAL 模式 |

## 下一版本规划

查看 `CHANGELOG.md` 底部的 "待办（下一版本候选）" 章节。
