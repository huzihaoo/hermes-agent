# Admission Control Usage Examples

## Example 1: Basic Setup

### Step 1: Configure User Roles

```bash
cat > ~/.hermes/config/user-roles.json << 'EOF'
{
  "users": {
    "default": "member",
    "张三": "owner",
    "李四": "admin",
    "王五": "member"
  },
  "user_id_mapping": {
    "ou_abc123": "张三",
    "ou_def456": "李四",
    "ou_xyz789": "王五"
  },
  "command_patterns": {
    "dangerous": [],
    "delete_small": [],
    "write": [],
    "read": []
  },
  "permission_matrix": {
    "owner": {"read": "ALLOW", "write": "ALLOW", "delete_small": "ALLOW", "dangerous": "CONFIRM"},
    "admin": {"read": "ALLOW", "write": "ALLOW", "delete_small": "CONFIRM", "dangerous": "APPROVE"},
    "member": {"read": "ALLOW", "write": "CONFIRM", "delete_small": "APPROVE", "dangerous": "DENY"}
  }
}
EOF
```

### Step 2: Enable Admission Control

```yaml
# ~/.hermes/config.yaml
platforms:
  feishu:
    extra:
      admission_control_enabled: true
```

### Step 3: Start Gateway

```bash
cd ~/.hermes/hermes-agent
python -m gateway.run
```

Expected output:
```
[admission] Controller initialized (db=~/.hermes/admission/queue.db, audit=~/.hermes/audit)
[admission] Configuration validated successfully
[worker] Starting queue workers for all lanes
[worker] Started fast lane worker
[worker] Started standard lane worker
[worker] Started heavy lane worker
[worker] Started cleanup loop
```

---

## Example 2: Message Flow

### Scenario: Three users send messages simultaneously

**User 1 (owner, priority 100):**
```
Message: "帮我查一下系统状态"
→ Lane: standard
→ Priority: 100
→ Position: 1
```

**User 2 (member, priority 10):**
```
Message: "hi"
→ Lane: fast
→ Priority: 10
→ Position: 1
```

**User 3 (member, priority 10):**
```
Message: "帮我写代码实现排序算法"
→ Lane: heavy
→ Priority: 10
→ Position: 1
```

**Processing:**
- All 3 messages process **in parallel** (different lanes)
- Within each lane, higher priority goes first
- Fast lane completes first (~1s)
- Standard lane completes second (~3s)
- Heavy lane completes last (~10s)

**Logs:**
```
[admission] Admitted user=user1 lane=standard pos=1 priority=100
[admission] Admitted user=user2 lane=fast pos=1 priority=10
[admission] Admitted user=user3 lane=heavy pos=1 priority=10
[worker] Processing xxx from fast lane (user=user2)
[worker] Processing yyy from standard lane (user=user1)
[worker] Processing zzz from heavy lane (user=user3)
[worker] Completed xxx in 1.23s
[worker] Completed yyy in 3.45s
[worker] Completed zzz in 10.67s
```

---

## Example 3: Priority Ordering

### Scenario: Multiple users in same lane

**Queue state:**
```
standard lane:
  1. user1 (owner, priority 100) - "查询数据"
  2. user2 (admin, priority 50) - "分析报告"
  3. user3 (member, priority 10) - "帮我看看"
  4. user4 (member, priority 10) - "有个问题"
```

**Processing order:**
1. user1 (priority 100) - processed first
2. user2 (priority 50) - processed second
3. user3 (priority 10) - processed third (FIFO within same priority)
4. user4 (priority 10) - processed fourth

**Check queue status:**
```bash
python -m gateway.admission.cli status
```

Output:
```
=== Admission Queue Status ===

[FAST] 0 pending
  (empty)

[STANDARD] 4 pending
  - user1 (owner, pri=100)
  - user2 (admin, pri=50)
  - user3 (member, pri=10)
  - user4 (member, pri=10)

[HEAVY] 0 pending
  (empty)

=== Metrics ===
Total admitted:  4
Total completed: 0
Total failed:    0
```

---

## Example 4: Queue Depth Monitoring

### Scenario: High load triggers warnings

**Simulate load:**
```python
# Send 15 messages to standard lane
for i in range(15):
    send_message(f"user{i}", "帮我查一下问题")
```

**Logs:**
```
[admission] Admitted user=user0 lane=standard pos=1
[admission] Admitted user=user1 lane=standard pos=2
...
[admission] Admitted user=user10 lane=standard pos=11
[admission] WARNING: standard lane depth=11 (threshold=10)
[admission] Admitted user=user11 lane=standard pos=12
...
```

**Check stats:**
```bash
python -m gateway.admission.cli stats
```

Output:
```
=== Admission Control Statistics ===

Total admitted:  15
Total completed: 3
Total failed:    0

Success rate: 100.0%
Failure rate: 0.0%

=== Current Queue Depth ===
Fast: 0
Standard: 12
Heavy: 0
Total pending: 12
```

---

## Example 5: Auto-Cleanup

### Scenario: Old items are automatically cleaned up

**Initial state (after 1 day):**
```bash
python -m gateway.admission.cli stats
```

Output:
```
Total admitted:  100
Total completed: 95
Total failed:    5
```

**After 6 hours (cleanup runs):**

Logs:
```
[worker] Cleaned up 80 old items
```

**New state:**
```
Total admitted:  100  # Metrics preserved
Total completed: 95
Total failed:    5
# But only recent 20 items kept in memory
```

---

## Example 6: Troubleshooting

### Scenario: Queue stuck, no processing

**Symptom:**
```bash
python -m gateway.admission.cli status
```

Output shows items stuck in queue for >5 minutes.

**Diagnosis:**
```bash
# Check worker status
grep "Queue worker" ~/.hermes/logs/gateway.log | tail -5

# Check for errors
grep -i error ~/.hermes/logs/gateway.log | grep admission | tail -10
```

**Solution 1: Restart gateway**
```bash
# Stop gateway (Ctrl+C)
# Start again
python -m gateway.run
```

**Solution 2: Clear queue (testing only)**
```bash
python -m gateway.admission.cli clear
```

---

## Example 7: Performance Tuning

### Scenario: Heavy lane is too slow

**Observation:**
```bash
grep "Completed.*heavy" ~/.hermes/logs/gateway.log | tail -10
```

Output shows heavy lane items taking >30s each.

**Analysis:**
```bash
python -m gateway.admission.cli status --json | jq '.heavy'
```

**Options:**

1. **Adjust lane classification** (if messages are misclassified):
   Edit `gateway/admission/controller.py`:
   ```python
   def _classify_lane(message: str) -> Lane:
       # Adjust keywords or length threshold
       if len(message) <= 10:  # Was 8
           return "fast"
   ```

2. **Add more workers** (requires code change):
   Edit `gateway/admission/worker.py`:
   ```python
   self._tasks = [
       asyncio.create_task(self._worker_loop("fast")),
       asyncio.create_task(self._worker_loop("standard")),
       asyncio.create_task(self._worker_loop("heavy")),
       asyncio.create_task(self._worker_loop("heavy")),  # Add 2nd heavy worker
       asyncio.create_task(self._cleanup_loop()),
   ]
   ```

3. **Optimize processing** (application-level):
   - Cache frequently accessed data
   - Use async I/O for external calls
   - Reduce LLM token usage

---

## Example 8: Audit Trail

### Scenario: Investigate why a message was rejected

**Check audit logs:**
```bash
tail -f ~/.hermes/audit/$(date +%Y-%m-%d).jsonl | jq .
```

Output:
```json
{
  "timestamp": "2026-04-25T10:30:45Z",
  "user_id": "user123",
  "action": "admit",
  "resource": "msg_abc",
  "result": "denied",
  "metadata": {
    "reason": "rate_limit_exceeded",
    "user_role": "member"
  }
}
```

**Search for specific user:**
```bash
grep "user123" ~/.hermes/audit/*.jsonl | jq .
```

---

## Example 9: Integration with Existing Code

### Scenario: Add admission control to custom platform

```python
from gateway.admission import AdmissionController
from gateway.admission.worker import QueueWorker

class MyPlatformAdapter:
    def __init__(self, config):
        self.admission = AdmissionController()
        self.worker = QueueWorker(
            self.admission,
            self._process_message
        )
    
    async def connect(self):
        await self.worker.start()
    
    async def disconnect(self):
        await self.worker.stop()
    
    async def handle_message(self, user_id, message):
        # Admit to queue
        admitted, feedback, item = await self.admission.admit(
            user_id=user_id,
            message=message,
            platform="my_platform"
        )
        
        if not admitted:
            return f"Rejected: {feedback}"
        
        return feedback  # "已加入队列..."
    
    async def _process_message(self, item):
        # Your actual message processing logic
        result = await self.process(item.message)
        return {"output": result}
```

---

## Example 10: Monitoring Dashboard (Future)

### Scenario: Real-time monitoring

**Current (CLI-based):**
```bash
watch -n 5 'python -m gateway.admission.cli stats'
```

**Future (Web dashboard):**
```
http://localhost:8080/admission/dashboard

Displays:
- Real-time queue depth per lane
- Processing time histogram
- Success/failure rate chart
- Top users by volume
- Alert history
```

---

## Summary

These examples cover:
- ✅ Basic setup and configuration
- ✅ Message flow and lane classification
- ✅ Priority ordering
- ✅ Queue depth monitoring
- ✅ Auto-cleanup
- ✅ Troubleshooting
- ✅ Performance tuning
- ✅ Audit trail investigation
- ✅ Integration with custom code
- ✅ Future monitoring capabilities

For more details, see:
- `gateway/admission/README.md` - Architecture and design
- `.hermes/plans/ADMISSION_DEPLOYMENT_CHECKLIST.md` - Deployment guide
- `.hermes/plans/ADMISSION_CONTROL_SUMMARY.md` - Implementation summary
