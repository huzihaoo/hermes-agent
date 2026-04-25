# Hermes 可观测性 MVP - Phase A 完成报告

**日期：** 2026-04-25  
**状态：** Phase A 完成，Phase B-F 待实施  
**Git Commit：** 2c6fbc38

---

## Phase A 完成情况

### ✅ 已完成

#### 1. Trace/Span 数据模型
- **文件：** `gateway/observability/trace.py`
- **功能：**
  - `Trace` 类：完整请求追踪（trace_id, user_id, platform, spans, total_tokens, total_cost_usd）
  - `Span` 类：单步追踪（span_id, name, kind, duration_ms, input_tokens, output_tokens, cost_usd）
  - 自动聚合 Token 和成本
- **测试：** 12 个测试，100% 通过

#### 2. SQLite 持久化存储
- **文件：** `gateway/observability/store.py`
- **功能：**
  - `TraceStore` 类：SQLite 存储
  - `save()`, `get()`, `list_recent()`, `get_spans()`
  - `stats_daily()`, `stats_by_user()` 统计查询
- **Schema：** traces 表 + spans 表 + 索引
- **测试：** 包含在 12 个测试中

#### 3. Token 成本计算
- **文件：** `gateway/observability/pricing.py`
- **功能：**
  - 12 个模型的定价（Claude, GPT, Gemini, DeepSeek）
  - `calculate_cost(input_tokens, output_tokens, model)` 
  - 自动处理 provider 前缀（如 `anthropic/claude-opus-4-6`）
- **测试：** 6 个测试，100% 通过

#### 4. 可观测性中间件
- **文件：** `gateway/observability/middleware.py`
- **功能：**
  - `TraceContext` 类：线程本地 trace 上下文
  - `trace_llm_call()`, `trace_tool_call()` 装饰器
  - `init_observability()`, `get_store()` 全局管理
- **测试：** 1 个集成测试，通过

#### 5. CLI 命令
- **文件：** `hermes_cli/trace.py`, `cli.py`, `hermes_cli/commands.py`
- **功能：**
  - `/trace list` - 列出最近 Trace（带图标、耗时、Token、成本）
  - `/trace show <id>` - 显示 Trace 详情和 Span 树
  - `/cost [--days N] [--group-by user]` - 成本统计
- **测试：** 4 个测试，100% 通过

### 📊 测试覆盖

```
tests/gateway/observability/
├── test_trace_store.py          12 passed
├── test_pricing.py               6 passed
└── test_middleware_integration.py 1 passed

tests/hermes_cli/
└── test_trace.py                 4 passed

总计: 23 passed
```

### 🎯 Phase A 验收标准

- [x] 每个请求有唯一 Trace ID
- [x] 每个 LLM 调用有 Span（含 Token 和成本）
- [x] 每个 Tool 调用有 Span（含耗时）
- [x] 数据持久化到 SQLite
- [x] `hermes trace list` 显示最近 Trace
- [x] `hermes trace show <id>` 显示 Span 树
- [x] 20+ 测试，100% 通过 ✅ (23 个测试)

---

## Phase B-F 实施方案

### Phase B: Dashboard + CLI 增强（Week 3-4）

#### B.1 Dashboard REST API

**新增文件：** `hermes_cli/web_server.py` (已存在，扩展)

**新增 API 端点：**

```python
# GET /api/traces?limit=20&user_id=xxx&status=completed
@app.get("/api/traces")
async def list_traces(limit: int = 20, user_id: Optional[str] = None, status: Optional[str] = None):
    store = get_store()
    traces = store.list_recent(limit=limit, user_id=user_id)
    if status:
        traces = [t for t in traces if t["status"] == status]
    return {"traces": traces}

# GET /api/traces/{trace_id}
@app.get("/api/traces/{trace_id}")
async def get_trace(trace_id: str):
    store = get_store()
    trace = store.get(trace_id)
    if not trace:
        raise HTTPException(404, "Trace not found")
    spans = store.get_spans(trace_id)
    return {"trace": trace, "spans": spans}

# GET /api/stats/daily?days=7
@app.get("/api/stats/daily")
async def stats_daily(days: int = 7):
    store = get_store()
    return store.stats_daily(days=days)

# GET /api/stats/cost?days=30&group_by=user
@app.get("/api/stats/cost")
async def stats_cost(days: int = 30, group_by: Optional[str] = None):
    store = get_store()
    if group_by == "user":
        return {"by_user": store.stats_by_user(days=days)}
    return store.stats_daily(days=days)
```

**测试：**
```python
# tests/hermes_cli/test_web_api.py
def test_api_list_traces(client):
    response = client.get("/api/traces?limit=10")
    assert response.status_code == 200
    assert "traces" in response.json()

def test_api_get_trace(client, sample_trace_id):
    response = client.get(f"/api/traces/{sample_trace_id}")
    assert response.status_code == 200
    assert "trace" in response.json()
    assert "spans" in response.json()
```

#### B.2 Dashboard 前端

**技术栈：** React (已有 `web/` 目录)

**新增页面：**

1. `/traces` - Trace 列表页
   - 表格：Trace ID, 用户, 时间, 耗时, Token, 成本, 状态
   - 筛选：用户, 状态, 时间范围
   - 分页

2. `/traces/:id` - Trace 详情页
   - Timeline 可视化（横向时间轴）
   - Span 树（嵌套显示）
   - 成本分解饼图

3. `/stats` - 统计 Dashboard
   - 今日概览卡片（任务数, Token, 成本）
   - 成本趋势图（7 天）
   - 按用户成本排行

**实施步骤：**
1. 复用现有 `web/` React 项目
2. 添加 3 个新路由
3. 使用 Chart.js 或 Recharts 绘图
4. 使用 Tailwind CSS 样式

---

### Phase C: 记忆系统升级（Week 5-6）

#### C.1 ChromaDB 向量索引

**安装依赖：**
```bash
pip install chromadb sentence-transformers
```

**新增文件：** `agent/memory_v2/`

```python
# agent/memory_v2/vector_store.py
import chromadb
from chromadb.config import Settings

class VectorMemoryStore:
    def __init__(self, persist_dir: Path):
        self.client = chromadb.Client(Settings(
            persist_directory=str(persist_dir),
            anonymized_telemetry=False
        ))
        self.collection = self.client.get_or_create_collection("hermes_memory")
    
    def add(self, memory_id: str, text: str, metadata: dict):
        self.collection.add(
            ids=[memory_id],
            documents=[text],
            metadatas=[metadata]
        )
    
    def search(self, query: str, limit: int = 5):
        results = self.collection.query(
            query_texts=[query],
            n_results=limit
        )
        return results
```

#### C.2 混合检索器

```python
# agent/memory_v2/retriever.py
class HybridRetriever:
    def __init__(self, vector_store, fts_store):
        self.vector_store = vector_store
        self.fts_store = fts_store
    
    def search(self, query: str, limit: int = 5):
        # 1. 语义搜索
        semantic = self.vector_store.search(query, limit=limit * 2)
        
        # 2. 关键词搜索
        keyword = self.fts_store.search(query, limit=limit * 2)
        
        # 3. RRF 混合排序
        return self._rrf_merge(semantic, keyword, limit=limit)
    
    def _rrf_merge(self, list1, list2, limit, k=60):
        """Reciprocal Rank Fusion"""
        scores = {}
        for rank, item in enumerate(list1, 1):
            scores[item["id"]] = scores.get(item["id"], 0) + 1 / (k + rank)
        for rank, item in enumerate(list2, 1):
            scores[item["id"]] = scores.get(item["id"], 0) + 1 / (k + rank)
        
        sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_items[:limit]
```

#### C.3 记忆演化

```python
# agent/memory_v2/evolution.py
class MemoryEvolution:
    def merge_similar(self, threshold=0.9):
        """合并相似度 > threshold 的记忆"""
        pass
    
    def decay_unused(self, days=30):
        """降低 N 天未访问的记忆权重"""
        pass
    
    def promote_frequent(self):
        """提升高频访问记忆的权重"""
        pass
```

---

### Phase D: 多租户基础（Week 7-8）

#### D.1 并发控制

```python
# gateway/concurrency/limiter.py
import asyncio

class UserConcurrencyLimiter:
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        self._active: dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, user_id: str) -> bool:
        async with self._lock:
            current = self._active.get(user_id, 0)
            if current >= self.max_concurrent:
                return False
            self._active[user_id] = current + 1
            return True
    
    async def release(self, user_id: str):
        async with self._lock:
            self._active[user_id] = max(0, self._active.get(user_id, 0) - 1)
```

#### D.2 Token 配额

```python
# gateway/concurrency/quota.py
class TokenQuotaManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()
    
    def check_quota(self, user_id: str) -> bool:
        """检查用户是否还有配额"""
        usage = self._get_monthly_usage(user_id)
        limit = self._get_user_limit(user_id)
        return usage < limit
    
    def consume(self, user_id: str, tokens: int):
        """消耗配额"""
        pass
```

---

### Phase E: 集成测试 + 文档 + 上线（Week 9-10）

#### E.1 端到端测试

```python
# tests/integration/test_observability_e2e.py
def test_full_agent_run_with_tracing():
    """完整 Agent 运行 + Trace 记录"""
    # 1. 启动 Agent
    # 2. 发送请求
    # 3. 验证 Trace 记录
    # 4. 验证成本计算
    pass
```

#### E.2 文档

- [ ] 可观测性用户指南
- [ ] API 参考文档
- [ ] 运维手册

#### E.3 上线

- [ ] 灰度发布（2-3 个用户）
- [ ] 监控告警
- [ ] 回滚方案

---

### Phase F: 反馈迭代 + 优化（Week 11-12）

- [ ] 收集用户反馈
- [ ] 性能优化（Trace 开销 < 5ms）
- [ ] Bug 修复

---

## 下一步行动

1. **立即：** 实施 Phase B Dashboard API（2 天）
2. **本周：** 完成 Phase B 前端（3 天）
3. **下周：** 启动 Phase C 记忆升级（5 天）

---

**文档维护者：** Kiro  
**最后更新：** 2026-04-25
