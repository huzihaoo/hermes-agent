# Admission v1.8.0 实施总结

**版本**: v1.8.0  
**发布日期**: 2026-04-27  
**主题**: VM 仓库多用户并发隔离

---

## 概述

本版本为 Hermes gateway admission 模块新增 VM 侧 git worktree 多用户隔离能力，支持 5 个飞书用户在 4 个主仓库 + 5 个嵌套仓库上并发开发，互不干扰。

**核心能力**：
- worktree 操作审计链（user → worktree → branch → git 操作）
- worktree 生命周期管理（自动创建、列表、GC、状态查询）
- VM 侧运维脚本集（审计日志、安全 push、GC、配额检查、日志轮转）

---

## 新增文件

### admission 模块

| 文件 | 说明 |
|------|------|
| `gateway/admission/worktree_audit.py` | worktree 审计日志模块（72 行） |
| `gateway/admission/worktree_manager.py` | worktree 管理 CLI（237 行） |

### VM 侧脚本（部署到 192.168.26.174）

| 文件 | 路径 | 说明 |
|------|------|------|
| `audit-logger.sh` | `/home/mini/worktrees/` | 审计日志记录 |
| `safe-push.sh` | `/home/mini/worktrees/` | 并发 push 安全包装器 |
| `gc-worktrees.sh` | `/home/mini/worktrees/` | worktree GC |
| `check-disk-quota.sh` | `/home/mini/worktrees/` | 磁盘配额检查 |
| `rotate-audit-log.sh` | `/home/mini/worktrees/` | 审计日志轮转 |

---

## 修改文件

| 文件 | 变更 |
|------|------|
| `~/.hermes/config/user-roles.json` | 新增 `repo_config` 段 + 补全王中坤映射 |
| `~/.hermes/config.yaml` | system_prompt 注入 worktree 路由规则 |
| `~/.hermes/workspace-work/AGENTS.md` | 同步 VM 仓库访问规则 |
| `gateway/admission/__init__.py` | 版本号 1.7.0 → 1.8.0 |
| `gateway/admission/CHANGELOG.md` | 新增 [1.8.0] 条目 |

---

## 功能详解

### 1. worktree_audit.py

**核心函数**：
- `log_worktree_event(user, repo, action, ...)` — 记录审计事件到 `~/.hermes/audit/worktree/YYYY-MM-DD.jsonl`
- `query_user_activity(user, days, repo)` — 查询用户近 N 天的操作记录

**审计覆盖范围**：
- 主仓库 git 操作（checkout、commit、merge、push）
- 嵌套仓库 git 操作（msg/data_proto、tools/*）
- worktree 创建/删除

**日志格式**：
```json
{
  "timestamp": "2026-04-27T15:27:17.123456",
  "user": "陈玉",
  "repo": "minieye_dnp_nop",
  "action": "checkout",
  "branch": "dev-nop-wp",
  "worktree_path": "/home/mini/worktrees/minieye_dnp_nop/陈玉",
  "session_id": "20260427_152717_abc123"
}
```

### 2. worktree_manager.py

**CLI 命令**：
```bash
# 自动创建 worktree（不存在时）
python3 worktree_manager.py ensure 陈玉 minieye_dnp_nop --branch dev-nop

# 列出所有 worktree
python3 worktree_manager.py list

# 列出某用户的 worktree
python3 worktree_manager.py list --user 陈玉

# 列出 30 天未访问的 stale worktree
python3 worktree_manager.py gc --older-than 30

# 查询 worktree 状态
python3 worktree_manager.py status 陈玉 minieye_dnp_nop
```

**配置读取**：
- 从 `~/.hermes/config/user-roles.json` 读取 `repo_config`
- 支持 `worktree_base`、`repos`（source、default_branch）

### 3. VM 侧运维脚本

| 脚本 | 触发方式 | 功能 |
|------|----------|------|
| `audit-logger.sh` | system_prompt 引导 agent 调用 | 记录 git 操作到 `/home/mini/worktrees/.audit.log` |
| `safe-push.sh` | senior 用户 merge 前调用 | 文件锁 + fetch + rebase + push |
| `gc-worktrees.sh` | cron（每周日凌晨 3 点） | 清理 30 天未访问的 worktree |
| `check-disk-quota.sh` | cron（每 6 小时） | 检查 10GB/用户/仓库 配额 |
| `rotate-audit-log.sh` | cron（每天凌晨 2 点） | 轮转 10MB 审计日志，保留 90 天 |

---

## 部署状态

### VM 侧（192.168.26.174）

**worktree 结构**：
```
/home/mini/worktrees/
├── minieye_dnp_nop/
│   ├── 陈玉/
│   ├── 刘旭/
│   ├── 王平/
│   └── 王中坤/
├── dnp_develop_enviroment/
│   ├── 陈玉/
│   ├── 刘旭/
│   ├── 王平/
│   └── 王中坤/
├── dnp_parking_dev_scripts/
│   ├── 陈玉/
│   ├── 刘旭/
│   ├── 王平/
│   └── 王中坤/
├── pnc_specs/
│   ├── 陈玉/
│   ├── 刘旭/
│   ├── 王平/
│   └── 王中坤/
├── audit-logger.sh
├── safe-push.sh
├── gc-worktrees.sh
├── check-disk-quota.sh
├── rotate-audit-log.sh
└── .audit.log
```

**嵌套仓库 symlink**（每个 worktree 内）：
```
/home/mini/worktrees/minieye_dnp_nop/陈玉/
├── msg/data_proto -> /home/mini/minieye_dnp_nop/msg/data_proto
├── tools/mcap_data_translate -> /home/mini/minieye_dnp_nop/tools/mcap_data_translate
├── tools/data_preprocess -> /home/mini/minieye_dnp_nop/tools/data_preprocess
├── tools/quality-gate-keeper -> /home/mini/minieye_dnp_nop/tools/quality-gate-keeper
└── tools/simulator_with_dnp -> /home/mini/minieye_dnp_nop/tools/simulator_with_dnp
```

**磁盘使用**：
- worktrees 总大小：12GB
- 单用户 worktree：~2.9GB
- 磁盘总量：1.5TB（充裕）

### Mac 侧（配置）

**user-roles.json**：
```json
{
  "repo_config": {
    "worktree_base": "/home/mini/worktrees",
    "owner_uses_main": true,
    "repos": {
      "minieye_dnp_nop": {
        "source": "/home/mini/minieye_dnp_nop",
        "default_branch": "dev-nop"
      },
      "dnp_develop_enviroment": {
        "source": "/home/mini/dnp_develop_enviroment",
        "default_branch": "dev"
      },
      "dnp_parking_dev_scripts": {
        "source": "/home/mini/dnp_parking_dev_scripts",
        "default_branch": "master_j6e"
      },
      "pnc_specs": {
        "source": "/home/mini/pnc_specs",
        "default_branch": "main"
      }
    }
  }
}
```

**config.yaml system_prompt**（关键段）：
- 路径路由：owner 走主 repo，其他人走 worktree
- 审计日志：所有 git 操作前记录
- 合并流程：senior 直接 push，member 需审批
- 并发 push 冲突检测：safe-push.sh
- 嵌套仓库审计：共享仓库也要记录

---

## 验证结果

### 端到端测试（10/10 通过）

| 测试 | 结果 |
|------|------|
| worktree 结构验证（16 个） | ✅ |
| 嵌套仓库 symlink 验证（5 个） | ✅ |
| 审计日志脚本验证 | ✅ |
| GC 脚本验证 | ✅ |
| 磁盘配额检查脚本验证 | ✅ |
| 安全 push 包装器验证 | ✅ |
| worktree git 状态验证 | ✅ |
| 嵌套仓库 git 状态验证 | ✅ |
| 审计日志轮转脚本验证 | ✅ |
| 磁盘使用统计 | ✅ (12GB) |

### 真实场景测试（4/4 通过）

| 场景 | 结果 |
|------|------|
| 新用户 worktree 自动创建 | ✅ |
| 嵌套仓库审计记录 | ✅ |
| 磁盘配额检查（2.9GB < 10GB） | ✅ |
| GC dry-run | ✅ |

### gstack Boil the Lake 审视

**完整度评分**：

| 维度 | 初始 | 最终 | 理想 |
|------|------|------|------|
| 隔离完整性 | 7/10 | 9/10 | 10/10 |
| 审计完整性 | 6/10 | 9/10 | 10/10 |
| 冲突检测 | 2/10 | 9/10 | 10/10 |
| 资源管理 | 2/10 | 8/10 | 10/10 |
| 用户体验 | 5/10 | 8/10 | 10/10 |
| 可观测性 | 4/10 | 8/10 | 10/10 |
| **总分** | **4.3/10** | **8.5/10** | **10/10** |

**QCon 交叉验证**：
- ✅ 与蚂蚁 Vibe Coding 的 WorkTree 隔离实践对齐
- ✅ 与阿里 OpenSandbox 的 workspace isolation + 冲突控制思路对齐
- ✅ 与字节 MarsCode 的审计链思路对齐

---

## 运维

### cron 任务（owner 手动添加）

```bash
# SSH 到 VM 后添加 cron
crontab -e

# 添加以下 3 行：
0 3 * * 0 /home/mini/worktrees/gc-worktrees.sh
0 2 * * * /home/mini/worktrees/rotate-audit-log.sh
0 */6 * * * /home/mini/worktrees/check-disk-quota.sh
```

### 日常监控

```bash
# 审计日志
tail -50 /home/mini/worktrees/.audit.log

# GC 日志
tail -20 /home/mini/worktrees/.gc.log

# 配额告警
tail -20 /home/mini/worktrees/.quota-alerts.log

# 磁盘使用
du -sh /home/mini/worktrees/*
```

### 新用户加入

1. `user-roles.json` 添加用户映射和角色
2. `user_overrides/` 创建用户配置文件
3. 用户首次使用时 agent 自动创建 worktree

---

## 回滚

如果需要回滚到 v1.7.0：

```bash
cd ~/.hermes/hermes-agent
git checkout gateway/admission/__init__.py
git checkout gateway/admission/CHANGELOG.md
rm gateway/admission/worktree_audit.py
rm gateway/admission/worktree_manager.py
hermes gateway restart
```

VM 侧 worktree 和脚本不需要删除，只是不再被 gateway 调用。

---

## 后续迭代

| 优先级 | 项目 | 触发条件 |
|--------|------|----------|
| P1 | 真实用户对话验证 | 立即 |
| P2 | session pause/resume | 长任务中断 |
| P3 | worktree 健康检查 | .git 损坏 |
| P4 | 跨仓库操作协调 | 多仓库原子提交需求 |
| P5 | container-per-user | 用户数 > 10 |

---

## 文档

| 文档 | 路径 |
|------|------|
| 设计文档（最终版） | `knowledge/wiki/designs/vm-repo-isolation.md` |
| CHANGELOG | `knowledge/wiki/designs/vm-repo-isolation-CHANGELOG.md` |
| RELEASE | `knowledge/wiki/designs/vm-repo-isolation-RELEASE.md` |
| 实施总结 | `knowledge/wiki/implementations/vm-repo-isolation-summary.md` |
| 收尾检查清单 | `knowledge/wiki/implementations/vm-repo-isolation-checklist.md` |
| 最终交付报告 | `knowledge/wiki/implementations/vm-repo-isolation-final-delivery.md` |
| gstack 审视报告 | `knowledge/wiki/reviews/vm-repo-isolation-gstack-review.md` |
| 快速参考卡片 | `knowledge/wiki/quick-refs/vm-repo-access.md` |

---

**实施完成时间**: 2026-04-27  
**实施人员**: Kiro (AI Agent)  
**审视方法**: gstack Boil the Lake + QCon Beijing 2026-04 多租户 agent 平台实践  
**生产就绪度**: 8.5/10
