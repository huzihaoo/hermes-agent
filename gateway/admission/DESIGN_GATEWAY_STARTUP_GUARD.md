# Gateway 启动防护设计 — 杜绝进程堆积

## 问题定义

**现状**：
- `start_gateway()` 只检查 PID 文件记录的一个进程
- 如果有多个 gateway 进程（手动启动、LaunchAgent 竞态、旧进程清理慢），只能检测到 PID 文件里的那个
- 结果：新进程启动后，旧进程还在后台，累积堆积

**目标**：
- 启动前强制扫描所有 gateway 进程
- 如果发现多个进程，拒绝启动（或自动清理）
- 让堆积问题在设计阶段就不可能发生

---

## 设计方案

### 方案 A：启动前全局扫描 + 拒绝启动（保守）

**逻辑**：
```python
def get_all_gateway_processes() -> list[int]:
    """扫描所有 gateway 进程，返回 PID 列表"""
    result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
    pids = []
    for line in result.stdout.splitlines():
        if "hermes gateway run" in line and str(os.getpid()) not in line:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    pids.append(int(parts[1]))
                except ValueError:
                    pass
    return pids

async def start_gateway(...):
    # 现有的 PID 文件检查
    existing_pid = get_running_pid()
    
    # 新增：全局扫描
    all_gateway_pids = get_all_gateway_processes()
    
    if len(all_gateway_pids) > 1:
        logger.error(
            "Multiple gateway processes detected (%d total). "
            "This indicates a process pileup. "
            "Run 'hermes gateway stop' to clean up first.",
            len(all_gateway_pids)
        )
        print(f"\n❌ Multiple gateway processes detected:\n")
        for pid in all_gateway_pids:
            print(f"   PID {pid}")
        print(f"\nRun 'hermes gateway stop' to clean up, then restart.\n")
        return False
    
    if len(all_gateway_pids) == 1:
        # 单个进程，走现有的 --replace 逻辑
        if replace:
            terminate_pid(all_gateway_pids[0], force=False)
            # ... 等待退出
        else:
            # 拒绝启动
            return False
    
    # all_gateway_pids == 0，正常启动
    ...
```

**优点**：
- 简单直接，不会误杀
- 强制用户走 `hermes gateway stop` 清理流程

**缺点**：
- 用户体验差，需要手动清理
- 如果 LaunchAgent 自动重启，会陷入循环失败

---

### 方案 B：启动前全局扫描 + 自动清理（激进）

**逻辑**：
```python
async def start_gateway(...):
    all_gateway_pids = get_all_gateway_processes()
    
    if len(all_gateway_pids) > 0:
        if replace:
            logger.info(
                "Found %d existing gateway process(es), cleaning up with --replace.",
                len(all_gateway_pids)
            )
            for pid in all_gateway_pids:
                try:
                    terminate_pid(pid, force=False)
                except (ProcessLookupError, PermissionError):
                    pass
            
            # 等待所有进程退出
            for _ in range(20):  # 10 秒
                remaining = [p for p in all_gateway_pids if _is_process_alive(p)]
                if not remaining:
                    break
                await asyncio.sleep(0.5)
            else:
                # 还有进程存活，强制 kill
                for pid in remaining:
                    try:
                        terminate_pid(pid, force=True)
                    except (ProcessLookupError, PermissionError):
                        pass
            
            remove_pid_file()
            release_all_scoped_locks()
        else:
            # 不允许 replace，拒绝启动
            logger.error("Gateway already running (PID %s). Use --replace or stop first.", all_gateway_pids)
            return False
    
    # 所有旧进程已清理，正常启动
    ...
```

**优点**：
- 用户无感，自动清理
- LaunchAgent 自动重启时也能正常工作

**缺点**：
- 可能误杀用户正在调试的进程
- 如果有多个 HERMES_HOME（未来多 profile 场景），会误杀其他 profile 的 gateway

---

### 方案 C：启动前全局扫描 + 智能判断（推荐）

**逻辑**：
```python
def get_all_gateway_processes_with_home() -> list[tuple[int, str]]:
    """扫描所有 gateway 进程，返回 (PID, HERMES_HOME) 列表"""
    result = subprocess.run(["ps", "auxe"], capture_output=True, text=True)
    processes = []
    for line in result.stdout.splitlines():
        if "hermes gateway run" in line and str(os.getpid()) not in line:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    pid = int(parts[1])
                    # 从环境变量中提取 HERMES_HOME
                    hermes_home = None
                    for part in parts:
                        if "HERMES_HOME=" in part:
                            hermes_home = part.split("=", 1)[1]
                            break
                    processes.append((pid, hermes_home))
                except ValueError:
                    pass
    return processes

async def start_gateway(...):
    current_home = str(get_hermes_home())
    all_processes = get_all_gateway_processes_with_home()
    
    # 只关心同一个 HERMES_HOME 的进程
    same_home_pids = [pid for pid, home in all_processes if home == current_home]
    
    if len(same_home_pids) > 1:
        # 多个进程，堆积了
        if replace:
            logger.warning(
                "Detected %d gateway processes under HERMES_HOME=%s. "
                "Cleaning up all before starting new instance.",
                len(same_home_pids), current_home
            )
            for pid in same_home_pids:
                try:
                    terminate_pid(pid, force=False)
                except (ProcessLookupError, PermissionError):
                    pass
            
            # 等待清理
            for _ in range(20):
                remaining = [p for p in same_home_pids if _is_process_alive(p)]
                if not remaining:
                    break
                await asyncio.sleep(0.5)
            else:
                # 强制 kill
                for pid in remaining:
                    try:
                        terminate_pid(pid, force=True)
                    except (ProcessLookupError, PermissionError):
                        pass
            
            remove_pid_file()
            release_all_scoped_locks()
        else:
            logger.error(
                "Multiple gateway processes detected under HERMES_HOME=%s (PIDs: %s). "
                "This indicates a process pileup. Run 'hermes gateway stop' first.",
                current_home, same_home_pids
            )
            return False
    
    elif len(same_home_pids) == 1:
        # 单个进程，走现有逻辑
        existing_pid = same_home_pids[0]
        if replace:
            terminate_pid(existing_pid, force=False)
            # ... 等待退出
        else:
            logger.error("Gateway already running (PID %d). Use --replace or stop first.", existing_pid)
            return False
    
    # same_home_pids == 0，正常启动
    ...
```

**优点**：
- 兼容未来多 profile 场景（不同 HERMES_HOME 可以并存）
- `--replace` 时自动清理所有同 HERMES_HOME 的进程
- 不会误杀其他 profile 的 gateway

**缺点**：
- 依赖 `ps auxe` 输出环境变量（macOS 和 Linux 都支持，但格式可能不同）
- 如果环境变量被截断（`ps` 输出有长度限制），可能无法准确判断

---

### 方案 D：启动前全局扫描 + PID 文件多实例（未来扩展）

**设计**：
- 不再用单一的 `gateway.pid`，改用 `gateway-<timestamp>.pid` 或 `gateway-<uuid>.pid`
- 启动时扫描所有 `gateway-*.pid` 文件，验证进程是否存活
- 清理过期的 PID 文件，kill 所有存活的旧进程

**优点**：
- 不依赖 `ps` 输出解析
- 可以记录更多元数据（启动时间、启动参数、HERMES_HOME）

**缺点**：
- 需要重构 PID 文件管理逻辑
- 需要定期清理过期的 PID 文件（避免文件堆积）

---

## 推荐方案

**Phase 1（短期）**：方案 C（智能判断）
- 在 `start_gateway()` 中加入全局扫描逻辑
- `--replace` 时自动清理所有同 HERMES_HOME 的进程
- 不带 `--replace` 时，如果检测到多个进程，拒绝启动并提示用户

**Phase 2（中期）**：方案 D（多实例 PID 文件）
- 重构 PID 文件管理，支持多实例追踪
- 启动时自动清理过期的 PID 文件和僵尸进程
- 为未来多 profile 场景做准备

---

## 实施计划

### Phase 1：全局扫描防护

**文件修改**：
1. `gateway/status.py`：
   - 新增 `get_all_gateway_processes() -> list[int]`
   - 新增 `_is_process_alive(pid: int) -> bool`
   - 可选：新增 `get_all_gateway_processes_with_home() -> list[tuple[int, str]]`（如果要支持多 HERMES_HOME）

2. `gateway/run.py`：
   - 修改 `start_gateway()`，在现有 PID 文件检查后加入全局扫描
   - `--replace` 时，清理所有检测到的进程（而不只是 PID 文件里的那个）
   - 不带 `--replace` 时，如果检测到多个进程，拒绝启动

**测试**：
- 手动启动多个 `hermes gateway run` 进程，验证新启动会拒绝或自动清理
- 验证 `--replace` 能清理所有旧进程
- 验证 LaunchAgent 自动重启时不会堆积

**版本号**：
- admission v1.7.0（新功能：启动防护）
- 或者 gateway 本身升版本（如果 gateway 有独立版本号）

### Phase 2：多实例 PID 文件（可选）

**设计细节**：
- PID 文件命名：`gateway-<start_timestamp>.pid`
- 启动时扫描 `gateway-*.pid`，验证进程存活，清理过期文件
- 每个 PID 文件记录：`{"pid": ..., "hermes_home": ..., "start_time": ..., "argv": ...}`
- 定期清理（如每次启动时）：删除超过 7 天的 PID 文件

**兼容性**：
- 保留 `gateway.pid` 作为"主" PID 文件（向后兼容）
- 新的多实例 PID 文件作为辅助追踪

---

## 风险评估

### 方案 C 的风险

**风险 1：`ps auxe` 输出解析失败**
- **场景**：不同操作系统的 `ps` 输出格式不同
- **缓解**：
  - 优先用 `ps auxe`（macOS/Linux 都支持）
  - 如果解析失败，降级到方案 A（只检查 PID 文件）
  - 记录 warning 日志，提示用户手动清理

**风险 2：误杀正在调试的进程**
- **场景**：用户手动启动了一个 gateway 进程用于调试，LaunchAgent 又启动了一个
- **缓解**：
  - `--replace` 时打印清理的 PID 列表，让用户知道发生了什么
  - 提供 `--no-replace` 选项，强制拒绝启动（而不是自动清理）

**风险 3：HERMES_HOME 判断失败**
- **场景**：`ps auxe` 输出被截断，无法提取 HERMES_HOME
- **缓解**：
  - 如果无法提取 HERMES_HOME，假设是同一个（保守策略）
  - 或者降级到方案 A（只检查 PID 文件）

---

## 测试用例

### 测试 1：单个进程，正常启动
```bash
# 前置：无 gateway 进程
hermes gateway start
# 预期：启动成功
```

### 测试 2：单个进程，--replace
```bash
# 前置：已有一个 gateway 进程
hermes gateway run --replace
# 预期：kill 旧进程，启动新进程
```

### 测试 3：多个进程，--replace
```bash
# 前置：手动启动 3 个 gateway 进程
hermes gateway run --replace
# 预期：kill 所有旧进程，启动新进程
```

### 测试 4：多个进程，不带 --replace
```bash
# 前置：手动启动 3 个 gateway 进程
hermes gateway start
# 预期：拒绝启动，提示用户先清理
```

### 测试 5：LaunchAgent 自动重启
```bash
# 前置：gateway 崩溃，LaunchAgent 触发重启
# 同时手动启动了一个 gateway 进程
# 预期：LaunchAgent 的重启能清理手动启动的进程（因为 LaunchAgent 用 --replace）
```

---

## 文档更新

### 更新 RUNBOOK_GATEWAY_PROCESS_PILEUP.md

**新增章节**：
```markdown
## 设计防护（v1.7.0+）

从 v1.7.0 开始，gateway 启动时会自动扫描所有同 HERMES_HOME 的进程：

- **不带 --replace**：如果检测到多个进程，拒绝启动，提示用户先清理
- **带 --replace**：自动清理所有旧进程，然后启动新进程

这意味着进程堆积问题在设计上已经被杜绝，不再需要手动清理。

如果你仍然遇到堆积问题，说明：
1. 你的 gateway 版本 < v1.7.0，需要升级
2. 全局扫描逻辑失败（检查日志中的 warning）
```

### 更新 VERSION_MANAGEMENT.md

**新增版本规则**：
```markdown
## Gateway 核心逻辑变更

如果修改了 `gateway/run.py` 或 `gateway/status.py` 的核心逻辑（如启动、停止、PID 管理），
必须：
1. 升 MINOR 版本号（如 1.6.0 → 1.7.0）
2. 在 CHANGELOG 中详细记录变更
3. 更新相关 runbook 和文档
4. 跑完整测试套件（包括手动测试启动/停止/重启）
```

---

## 总结

**核心思路**：
- 不依赖单一 PID 文件，启动时全局扫描所有 gateway 进程
- `--replace` 时自动清理所有旧进程（而不只是 PID 文件里的那个）
- 不带 `--replace` 时，如果检测到多个进程，拒绝启动

**优先级**：
- Phase 1（方案 C）：短期内实施，解决当前堆积问题
- Phase 2（方案 D）：中期规划，为多 profile 场景做准备

**预期效果**：
- 进程堆积问题在设计上被杜绝
- 用户无需手动清理，`hermes gateway restart` 总是能正常工作
- 为未来多 profile 场景（不同 HERMES_HOME 并存）打好基础

---

**最后更新**: 2026-04-26  
**维护者**: 胡子豪  
**版本**: 1.0.0
