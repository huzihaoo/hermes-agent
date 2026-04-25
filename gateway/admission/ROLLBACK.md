# Admission Control — 回滚与稳定性保障

## 快速回滚能力

### 1. Git Tag 标记稳定版本

每个稳定版本必须打 tag，方便快速回滚。

```bash
cd /Users/songying/.hermes/hermes-agent

# 为当前稳定版本打 tag
git tag -a admission-v1.0.1 -m "admission v1.0.1 — 并发硬化 + 版本管理体系

- 62/62 tests passed
- 3 个并发压力测试通过
- WAL mode + upsert + metrics_lock
- 完整版本管理基础设施"

# 推送 tag（如果有远程仓库）
git push origin admission-v1.0.1

# 查看所有 admission 版本 tag
git tag -l "admission-v*"
```

### 2. 回滚到稳定版本

#### 场景 A：新版本有 bug，需要紧急回滚

```bash
# 1. 查看可用的稳定版本
git tag -l "admission-v*"
# 输出:
#   admission-v1.0.0
#   admission-v1.0.1

# 2. 查看目标版本的详细信息
git show admission-v1.0.1 --stat

# 3. 回滚到稳定版本
git checkout admission-v1.0.1 -- gateway/admission/ tests/gateway/test_admission_*.py

# 4. 验证
source venv/bin/activate
pytest tests/gateway/test_admission_*.py -q
python gateway/admission/version_check.py

# 5. 提交回滚
git commit -m "revert(admission): rollback to v1.0.1 (stable)

Reason: v1.0.2 has critical bug in _pop_round_robin
Rolled back to: admission-v1.0.1 (62/62 tests passed)

Tests: 62/62 passed"
```

#### 场景 B：只回滚特定文件

```bash
# 只回滚 queue.py 到 v1.0.1
git show admission-v1.0.1:gateway/admission/queue.py > gateway/admission/queue.py

# 验证
pytest tests/gateway/test_admission_queue.py -v
```

#### 场景 C：创建回滚分支进行测试

```bash
# 创建回滚测试分支
git checkout -b admission-rollback-test admission-v1.0.1

# 测试
pytest tests/gateway/test_admission_*.py -q

# 如果测试通过，合并回主分支
git checkout overlay/stable
git merge admission-rollback-test
```

### 3. 版本快照备份

每次升级前自动备份当前版本。

```bash
# 创建备份脚本
cat > gateway/admission/backup_before_upgrade.sh << 'EOF'
#!/bin/bash
# Admission 升级前备份脚本

BACKUP_DIR="$HOME/.hermes/admission-backups"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
CURRENT_VERSION=$(python3 -c "import sys; sys.path.insert(0, 'gateway'); from admission import __version__; print(__version__)")

mkdir -p "$BACKUP_DIR"

# 备份源码
tar czf "$BACKUP_DIR/admission-v${CURRENT_VERSION}-${TIMESTAMP}.tar.gz" \
    gateway/admission/ \
    tests/gateway/test_admission_*.py

# 备份数据库
if [ -f "$HOME/.hermes/admission/queue.db" ]; then
    cp "$HOME/.hermes/admission/queue.db" \
       "$BACKUP_DIR/queue-v${CURRENT_VERSION}-${TIMESTAMP}.db"
fi

echo "✓ Backup created: $BACKUP_DIR/admission-v${CURRENT_VERSION}-${TIMESTAMP}.tar.gz"
echo "✓ Current version: v${CURRENT_VERSION}"
EOF

chmod +x gateway/admission/backup_before_upgrade.sh
```

使用备份：

```bash
# 升级前备份
./gateway/admission/backup_before_upgrade.sh

# 如果升级失败，从备份恢复
cd /Users/songying/.hermes/hermes-agent
tar xzf ~/.hermes/admission-backups/admission-v1.0.1-*.tar.gz
```

### 4. 数据库回滚

admission 使用 SQLite 持久化，需要考虑数据兼容性。

#### 向后兼容的升级（PATCH/MINOR）

```bash
# 数据库自动兼容，无需特殊处理
# 只需回滚代码即可
git checkout admission-v1.0.1 -- gateway/admission/
```

#### 破坏性升级（MAJOR）

```bash
# v2.0.0 → v1.0.1 需要数据降级

# 1. 备份当前数据库
cp ~/.hermes/admission/queue.db ~/.hermes/admission/queue-v2.0.0-backup.db

# 2. 回滚代码
git checkout admission-v1.0.1 -- gateway/admission/

# 3. 运行降级脚本（如果有）
python gateway/admission/migrate_down_v2_to_v1.py

# 4. 验证
python gateway/admission/version_check.py
```

### 5. 金丝雀部署（生产环境）

如果 admission 在生产环境运行，使用金丝雀部署降低风险。

```bash
# 1. 在测试环境验证新版本
pytest tests/gateway/test_admission_*.py -q

# 2. 部署到 10% 流量
# （需要配合 gateway 的流量分配机制）

# 3. 监控关键指标
python -m gateway.admission.cli stats --watch

# 4. 如果指标正常，逐步扩大到 50% → 100%
# 5. 如果指标异常，立即回滚到稳定版本
```

---

## 稳定性保障 Checklist

### 升级前

- [ ] 当前版本已打 git tag
- [ ] 运行 `backup_before_upgrade.sh` 备份
- [ ] 数据库已备份（`~/.hermes/admission/queue.db`）
- [ ] 完整测试套件通过：`pytest tests/gateway/test_admission_*.py -q`
- [ ] 版本检查通过：`python gateway/admission/version_check.py`

### 升级后

- [ ] 新版本测试通过
- [ ] 版本检查通过
- [ ] 为新版本打 git tag
- [ ] 更新 CHANGELOG.md
- [ ] 监控运行指标（如果在生产环境）

### 回滚触发条件

立即回滚如果：
- 测试失败率 > 5%
- 生产环境出现数据丢失
- 队列深度异常增长（> 100）
- 处理延迟 > 10 秒
- 内存泄漏（RSS > 500MB）

---

## 版本兼容性矩阵

| 从版本 | 到版本 | 代码回滚 | 数据回滚 | 风险 |
|--------|--------|---------|---------|------|
| 1.0.1 → 1.0.2 | PATCH | ✓ 直接回滚 | ✓ 自动兼容 | 低 |
| 1.0.x → 1.1.0 | MINOR | ✓ 直接回滚 | ✓ 自动兼容 | 低 |
| 1.x.x → 2.0.0 | MAJOR | ✓ 需要降级脚本 | ⚠️ 需要数据迁移 | 高 |

---

## 快速命令参考

```bash
# 查看当前版本
python -c "from gateway.admission import __version__; print(__version__)"

# 查看所有稳定版本
git tag -l "admission-v*"

# 回滚到 v1.0.1
git checkout admission-v1.0.1 -- gateway/admission/ tests/gateway/test_admission_*.py

# 验证回滚
pytest tests/gateway/test_admission_*.py -q
python gateway/admission/version_check.py

# 查看备份
ls -lh ~/.hermes/admission-backups/

# 恢复备份
tar xzf ~/.hermes/admission-backups/admission-v1.0.1-*.tar.gz
```

---

## 紧急回滚 SOP

```bash
# 1. 停止服务（如果在运行）
# pkill -f "gateway.run"  # 或者 systemctl stop hermes-gateway

# 2. 回滚代码
cd /Users/songying/.hermes/hermes-agent
git checkout admission-v1.0.1 -- gateway/admission/ tests/gateway/test_admission_*.py

# 3. 回滚数据库（如果需要）
cp ~/.hermes/admission-backups/queue-v1.0.1-*.db ~/.hermes/admission/queue.db

# 4. 验证
source venv/bin/activate
pytest tests/gateway/test_admission_*.py -q
python gateway/admission/version_check.py

# 5. 重启服务
# python -m gateway.run  # 或者 systemctl start hermes-gateway

# 6. 记录回滚原因
git commit -m "revert(admission): emergency rollback to v1.0.1

Reason: [具体原因]
Rolled back from: v1.0.x
Tests: 62/62 passed"
```
