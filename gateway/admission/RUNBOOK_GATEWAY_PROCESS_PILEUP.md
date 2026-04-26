# Gateway 进程堆积问题 Runbook

## 问题现象

```bash
hermes gateway status
# 或者后台进程失败日志显示：
# WARNING gateway.run: Shutdown diagnostic — other hermes processes running:
#   songying  82177  ... /hermes
#   songying  99289  ... /hermes gateway run --replace
#   songying  99123  ... /hermes
```

多个 `hermes` 或 `hermes gateway run` 进程同时存在，导致：
- 新 gateway 启动失败（检测到已有实例）
- 资源占用累积（内存、CPU）
- 端口冲突或 API 不可达
- LaunchAgent 反复重启失败

---

## 根因分析

### 1. PID 文件机制的局限

**设计**：
- Gateway 启动时写入 `~/.hermes/gateway.pid`（JSON 格式，记录 PID、argv、start_time）
- `--replace` 模式会先读取 PID 文件，kill 旧进程，等待最多 10 秒，然后启动新进程
- 只能追踪**一个**"官方" gateway 进程

**问题**：
- 如果旧进程清理慢（正在处理请求、关闭连接、等待子任务），新进程可能在旧进程完全退出前就启动了
- 手动启动的多个 `hermes gateway run --replace` 会依次覆盖 PID 文件，但旧进程可能还在后台
- PID 文件只记录最后一个启动的进程，之前的"僵尸"进程不会被追踪

### 2. LaunchAgent 自动重启竞态

**配置**：`~/Library/LaunchAgents/ai.hermes.gateway.plist`
- `RunAtLoad=true`：系统登录时自动启动
- `KeepAlive=false`：进程退出后不自动重启（但 `hermes gateway restart` 会触发）

**竞态场景**：
1. 用户手动运行 `hermes gateway run --replace`（前台或后台）
2. LaunchAgent 检测到进程退出，触发重启
3. 两个启动请求几乎同时执行，都通过了 PID 文件检查
4. 结果：两个 gateway 进程同时运行

### 3. 手动启动的多次重试

**场景**：
- 用户发现 gateway 异常，连续执行 `hermes gateway run --replace`
- 每次启动都会 kill 上一个进程，但如果上一个进程还在清理中，新进程已经启动
- 累积效应：多个进程堆积

---

## 诊断步骤

### 1. 查看所有 hermes 进程

```bash
ps aux | grep -E "hermes|mem0" | grep -v grep
```

**关键字段**：
- `PID`：进程 ID
- `STAT`：进程状态（`S`=sleeping, `R`=running, `T`=stopped, `Z`=zombie）
- `TIME`：CPU 时间（判断进程是否活跃）
- `COMMAND`：完整命令行（区分 CLI、gateway、mem0-gateway）

**正常状态**：
- 1 个 `hermes gateway run` 进程（LaunchAgent 管理）
- 1 个 `mem0-gateway/server.py` 进程（独立服务）
- 0-N 个 `hermes` CLI 进程（用户交互会话）

**异常状态**：
- 多个 `hermes gateway run` 进程
- 多个 `hermes` CLI 进程处于 `T`（stopped）状态

### 2. 查看 PID 文件

```bash
cat ~/.hermes/gateway.pid
```

**输出示例**：
```json
{"pid": 964, "kind": "hermes-gateway", "argv": [...], "start_time": null}
```

**验证**：
```bash
ps -p 964 -o pid,stat,command
```

如果进程不存在或状态异常，说明 PID 文件已过期。

### 3. 查看 LaunchAgent 状态

```bash
launchctl list | grep hermes
hermes gateway status
```

**关键信息**：
- `Launchd PID`：LaunchAgent 管理的进程 PID
- `State`：running / stopped
- `Local API`：reachable / not reachable

---

## 解决方案

### ⚠️ v1.7.0+ 用户注意

**从 v1.7.0 开始，gateway 启动时会自动扫描所有同 HERMES_HOME 的进程：**

- **不带 --replace**：如果检测到多个进程，拒绝启动，提示用户先清理
- **带 --replace**：自动清理所有旧进程，然后启动新进程

**这意味着进程堆积问题在设计上已经被杜绝**，不再需要手动清理。

如果你仍然遇到堆积问题，说明：
1. 你的 gateway 版本 < v1.7.0，需要升级
2. 全局扫描逻辑失败（检查日志中的 warning）
3. 进程的 HERMES_HOME 环境变量不同（多 profile 场景）

**验证版本**：
```bash
cd ~/.hermes/hermes-agent
git log --oneline gateway/run.py | grep "v1.7.0\|startup guard" | head -1
```

如果看到 "v1.7.0" 或 "startup guard"，说明已经有防护。

---

### 方案 A：使用官方命令（推荐）

```bash
# 1. 停止所有 gateway 进程
hermes gateway stop

# 2. 验证清理完成
ps aux | grep "hermes gateway" | grep -v grep
# 应该为空

# 3. 重新启动
hermes gateway start

# 4. 验证状态
hermes gateway status
```

**原理**：
- `hermes gateway stop` 会：
  1. 读取 PID 文件，kill 官方进程
  2. 卸载 LaunchAgent（`launchctl unload`）
  3. 清理 PID 文件和 scoped locks
- `hermes gateway start` 会：
  1. 重新加载 LaunchAgent
  2. 启动新进程
  3. 写入新的 PID 文件

### 方案 B：手动清理（当方案 A 失败时）

```bash
# 1. 找出所有 gateway 进程
ps aux | grep "hermes gateway run" | grep -v grep | awk '{print $2}'

# 2. 逐个 kill（先 SIGTERM，再 SIGKILL）
kill <PID1> <PID2> ...
# 等待 5 秒
sleep 5
# 如果还在，强制 kill
kill -9 <PID1> <PID2> ...

# 3. 清理 PID 文件
rm -f ~/.hermes/gateway.pid

# 4. 卸载 LaunchAgent
launchctl unload ~/Library/LaunchAgents/ai.hermes.gateway.plist

# 5. 重新启动
hermes gateway start
```

### 方案 C：保留 CLI 会话，只清理 gateway

**场景**：有正在运行的 `hermes` CLI 会话（如 s000、s002），不想中断

```bash
# 1. 只 kill gateway 进程
ps aux | grep "hermes gateway run" | grep -v grep | awk '{print $2}' | xargs kill

# 2. 等待清理
sleep 5

# 3. 验证
ps aux | grep "hermes gateway" | grep -v grep
# 应该为空

# 4. 重启 gateway
hermes gateway start
```

**注意**：
- CLI 会话（`hermes` 不带 `gateway` 参数）是独立进程，不影响 gateway
- 但如果 CLI 会话处于 `T`（stopped）状态，建议手动 `fg` 恢复或 `kill`

---

## 预防措施

### 1. 避免手动启动 gateway

**原则**：
- 优先使用 `hermes gateway start/stop/restart`
- 避免直接运行 `hermes gateway run --replace`（除非调试）

**原因**：
- LaunchAgent 管理的进程有自动重启、日志收集、环境变量注入等功能
- 手动启动的进程不受 LaunchAgent 管理，容易产生孤儿进程

### 2. 检查 LaunchAgent 配置

```bash
cat ~/Library/LaunchAgents/ai.hermes.gateway.plist | grep -A2 "KeepAlive\|RunAtLoad"
```

**推荐配置**：
- `RunAtLoad=true`：登录时自动启动
- `KeepAlive=false`：不自动重启（避免崩溃循环）

**如果需要自动重启**：
```xml
<key>KeepAlive</key>
<dict>
  <key>SuccessfulExit</key>
  <false/>
</dict>
```

这样只有非正常退出（exit code != 0）才会重启。

### 3. 监控进程数量

**定期检查**：
```bash
# 每天或每周运行一次
ps aux | grep "hermes gateway run" | grep -v grep | wc -l
```

**预期值**：1

**如果 > 1**：
- 立即执行方案 A 清理
- 检查最近的操作日志（是否有手动启动、LaunchAgent 重启）

### 4. 日志审计

**Gateway 日志**：
```bash
tail -f ~/.hermes/logs/gateway.log
```

**关键事件**：
- `Replacing existing gateway instance (PID ...)`：触发了 `--replace`
- `Shutdown diagnostic — other hermes processes running`：退出时发现其他进程
- `Another gateway instance is already running`：启动失败

**LaunchAgent 日志**：
```bash
tail -f ~/Library/Logs/ai.hermes.gateway.stdout.log
tail -f ~/Library/Logs/ai.hermes.gateway.stderr.log
```

---

## 版本管控集成

### 文件位置

```
~/.hermes/hermes-agent/gateway/admission/
├── RUNBOOK_GATEWAY_PROCESS_PILEUP.md  # 本文档
├── VERSION_MANAGEMENT.md              # 版本管控规范
├── CHANGELOG.md                       # 变更历史
└── ...
```

### 更新规则

**何时更新本文档**：
- 发现新的进程堆积场景
- 解决方案失效或需要补充
- Gateway 启动机制变更（如 PID 文件格式、`--replace` 逻辑）

**更新流程**：
1. 编辑本文档
2. 在 `CHANGELOG.md` 中记录变更（如果涉及代码修改，升版本号）
3. Git commit：`docs(admission): update gateway process pileup runbook`

### 相关文档

- `gateway/run.py`：`start_gateway()` 函数，`--replace` 逻辑
- `gateway/status.py`：`get_running_pid()`, `terminate_pid()`, PID 文件管理
- `~/Library/LaunchAgents/ai.hermes.gateway.plist`：LaunchAgent 配置

---

## 附录：常见问题

### Q1: 为什么 `hermes gateway stop` 后还有进程？

**A**: 可能是：
1. **CLI 会话**：`hermes` 不带 `gateway` 参数的进程是独立的，不受 `stop` 影响
2. **mem0-gateway**：独立服务，需要单独停止（`pkill -f mem0-gateway`）
3. **僵尸进程**：进程已退出但未被回收，`ps` 显示 `Z` 状态，等待父进程回收

### Q2: `--replace` 为什么还会堆积？

**A**: `--replace` 只能 kill PID 文件记录的进程，如果：
- 旧进程清理慢，新进程已经启动
- 手动启动的进程没有更新 PID 文件
- LaunchAgent 和手动启动竞态

解决：使用方案 A 或 B 手动清理。

### Q3: 如何判断进程是否"活跃"？

**A**: 看 `TIME` 字段（CPU 时间）：
- 如果持续增长，说明进程在工作
- 如果长时间不变，可能是僵尸进程或卡住了

也可以用 `lsof -p <PID>` 查看打开的文件和网络连接。

### Q4: LaunchAgent 日志在哪里？

**A**:
- stdout: `~/Library/Logs/ai.hermes.gateway.stdout.log`
- stderr: `~/Library/Logs/ai.hermes.gateway.stderr.log`

如果文件不存在，说明 LaunchAgent 配置中没有指定 `StandardOutPath` 和 `StandardErrorPath`。

---

**最后更新**: 2026-04-26  
**维护者**: 胡子豪  
**版本**: 1.0.0
