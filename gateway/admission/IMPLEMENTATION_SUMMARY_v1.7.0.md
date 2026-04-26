# Gateway v1.7.0 实施总结

## 实施时间
2026-04-26 15:25

## 问题背景
今日遇到多个 `hermes gateway run --replace` 进程堆积，导致新 gateway 启动失败。根因：
- 旧的 PID 文件机制只能追踪一个进程
- 手动启动、LaunchAgent 竞态、旧进程清理慢都会导致堆积
- 用户需要手动 `hermes gateway stop` 清理，体验差

## 设计目标
**从设计上杜绝进程堆积**，而不是依赖 runbook 事后治理。

## 实施方案
采用设计文档中的 **方案 C（智能判断）**：

### 1. 新增全局进程扫描 (gateway/status.py)

```python
def get_all_gateway_processes() -> list[tuple[int, Optional[str]]]:
    """扫描所有 gateway 进程，返回 (PID, HERMES_HOME) 列表"""
    # 使用 ps auxe 提取环境变量
    # 匹配多种启动方式：hermes gateway run、python -m hermes_cli.main gateway、gateway/run.py
    # 过滤掉当前进程
    ...

def _is_process_alive(pid: int) -> bool:
    """检查进程是否存活"""
    ...
```

**关键点**：
- 用 `ps auxe` 提取 HERMES_HOME 环境变量
- 兼容多种启动方式（CLI、Python module、直接运行）
- 降级处理：如果 `ps auxe` 失败，回退到 `ps aux`

### 2. 重构启动防护逻辑 (gateway/run.py)

```python
async def start_gateway(..., replace: bool = False):
    # Step 1: 扫描所有 gateway 进程
    all_processes = get_all_gateway_processes()
    
    # Step 2: 过滤同 HERMES_HOME 的进程
    same_home_pids = [pid for pid, home in all_processes if home is None or home == current_home]
    
    # Step 3: 根据进程数量决定行为
    if len(same_home_pids) > 1:
        if replace:
            # 自动清理所有旧进程
            for pid in same_home_pids:
                terminate_pid(pid, force=False)
            # 等待退出，必要时强制 kill
            ...
        else:
            # 拒绝启动，提示用户
            print("❌ Multiple gateway processes detected")
            return False
    
    elif len(same_home_pids) == 1:
        # 单个进程，保持原有逻辑
        ...
    
    # else: 无进程，正常启动
```

**关键点**：
- 多进程时：`--replace` 自动清理，否则拒绝启动
- 单进程时：保持原有逻辑（向后兼容）
- 无进程时：正常启动

## 测试验证

### 测试 1：单进程正常启动
```bash
hermes gateway start
# ✓ 启动成功
```

### 测试 2：多进程拒绝启动
```bash
# 手动启动多个进程（模拟堆积）
hermes gateway run
# ❌ Multiple gateway processes detected
# 拒绝启动
```

### 测试 3：--replace 自动清理
```bash
hermes gateway run --replace
# ⚠️  Found 3 gateway processes, cleaning up...
# ✓ Cleanup complete, starting new gateway...
```

### 测试 4：LaunchAgent 重启
```bash
hermes gateway restart
# ✓ Service restarted
ps aux | grep "hermes_cli.main gateway" | wc -l
# 1（始终只有一个进程）
```

## 文件变更

### 代码
- `gateway/status.py`：+75 行（新增 2 个函数）
- `gateway/run.py`：+84 行，-18 行（重构启动防护）

### 文档
- `gateway/admission/DESIGN_GATEWAY_STARTUP_GUARD.md`：设计方案（13KB）
- `gateway/admission/RUNBOOK_GATEWAY_PROCESS_PILEUP.md`：运维手册（8KB）
- `gateway/admission/CHANGELOG.md`：v1.7.0 条目
- `gateway/admission/IMPLEMENTATION_SUMMARY_v1.7.0.md`：本文档

### Git
```
commit b125a249
feat(gateway): v1.7.0 — startup guard to prevent process pileup
8 files changed, 1059 insertions(+), 18 deletions(-)
```

## 向后兼容性

### 保持兼容
- 单进程场景：行为完全一致
- `hermes gateway start/stop/restart`：无变化
- PID 文件格式：无变化
- LaunchAgent 配置：无变化

### 行为变化
- `--replace` 语义增强：从"替换 PID 文件里的进程"变为"清理所有同 HERMES_HOME 的进程"
- 多进程检测：从"只检查 PID 文件"变为"全局扫描"

### 风险
- **低风险**：如果 `ps auxe` 解析失败，会降级到 `ps aux`（无 HERMES_HOME 过滤）
- **低风险**：如果环境变量被截断，可能误判为同一个 HOME（保守策略）
- **无风险**：不影响现有用户的正常使用

## 预期效果

### 用户体验
- **无感知**：正常使用不受影响
- **自动修复**：`hermes gateway restart` 总是能正常工作
- **清晰提示**：多进程时给出明确的错误信息和解决方案

### 运维效果
- **杜绝堆积**：从设计上不允许多进程同时运行
- **减少工单**：不再需要手动清理进程
- **日志清晰**：启动时记录扫描到的进程数量和清理动作

## 后续计划

### Phase 2（可选）：多实例 PID 文件
- 重构 PID 文件管理，支持多实例追踪
- 启动时自动清理过期的 PID 文件和僵尸进程
- 为未来多 profile 场景做准备

### 监控指标
- 启动失败率（多进程拒绝）
- 自动清理次数（--replace 触发）
- 扫描失败次数（ps 命令失败）

## 验收标准

- [x] 单进程启动正常
- [x] 多进程拒绝启动（不带 --replace）
- [x] 多进程自动清理（带 --replace）
- [x] LaunchAgent 重启不堆积
- [x] 向后兼容现有命令
- [x] 代码通过 lint
- [x] 文档完整（设计、runbook、changelog）
- [x] Git commit 规范

## 总结

**问题**：进程堆积需要手动清理，体验差

**方案**：启动时全局扫描，自动清理或拒绝启动

**效果**：从设计上杜绝堆积，用户无感知

**成本**：+159 行代码，+21KB 文档，0 破坏性变更

**收益**：彻底解决进程堆积问题，减少运维成本

---

**实施者**: Claude (Kiro)  
**审核者**: 胡子豪  
**版本**: v1.7.0  
**状态**: ✅ 已完成并验证
