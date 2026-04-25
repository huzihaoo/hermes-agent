#!/bin/bash
# Admission 升级前备份脚本

set -e

BACKUP_DIR="$HOME/.hermes/admission-backups"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REPO_ROOT="/Users/songying/.hermes/hermes-agent"

cd "$REPO_ROOT"

# 获取当前版本
CURRENT_VERSION=$(cd "$REPO_ROOT" && source venv/bin/activate 2>/dev/null; python3 -c "import sys; sys.path.insert(0, '.'); from gateway.admission import __version__; print(__version__)" 2>/dev/null || echo "unknown")

mkdir -p "$BACKUP_DIR"

echo "🔄 Backing up admission v${CURRENT_VERSION}..."

# 备份源码
tar czf "$BACKUP_DIR/admission-v${CURRENT_VERSION}-${TIMESTAMP}.tar.gz" \
    gateway/admission/ \
    tests/gateway/test_admission_*.py \
    2>/dev/null

# 备份数据库
if [ -f "$HOME/.hermes/admission/queue.db" ]; then
    cp "$HOME/.hermes/admission/queue.db" \
       "$BACKUP_DIR/queue-v${CURRENT_VERSION}-${TIMESTAMP}.db"
    echo "✓ Database backed up"
fi

# 备份审计日志（最近 7 天）
if [ -d "$HOME/.hermes/audit" ]; then
    find "$HOME/.hermes/audit" -name "*.jsonl" -mtime -7 | \
        tar czf "$BACKUP_DIR/audit-${TIMESTAMP}.tar.gz" -T - 2>/dev/null || true
    echo "✓ Audit logs backed up (last 7 days)"
fi

echo ""
echo "✅ Backup completed:"
echo "   Source: $BACKUP_DIR/admission-v${CURRENT_VERSION}-${TIMESTAMP}.tar.gz"
echo "   Version: v${CURRENT_VERSION}"
echo "   Time: $(date)"
echo ""
echo "To restore:"
echo "   cd $REPO_ROOT"
echo "   tar xzf $BACKUP_DIR/admission-v${CURRENT_VERSION}-${TIMESTAMP}.tar.gz"
