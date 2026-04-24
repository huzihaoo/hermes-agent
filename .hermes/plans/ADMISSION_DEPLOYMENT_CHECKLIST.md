# Admission Control Deployment Checklist

## Pre-Deployment

### 1. Configuration Files

- [ ] Create `~/.hermes/config/user-roles.json`
  ```bash
  mkdir -p ~/.hermes/config
  cat > ~/.hermes/config/user-roles.json << 'EOF'
  {
    "users": {
      "default": "member"
    },
    "user_id_mapping": {},
    "command_patterns": {
      "dangerous": [],
      "delete_small": [],
      "write": [],
      "read": []
    },
    "permission_matrix": {
      "owner": {"read": "ALLOW", "write": "ALLOW", "delete_small": "ALLOW", "dangerous": "CONFIRM"},
      "admin": {"read": "ALLOW", "write": "ALLOW", "delete_small": "CONFIRM", "dangerous": "APPROVE"},
      "senior": {"read": "ALLOW", "write": "ALLOW", "delete_small": "APPROVE", "dangerous": "DENY"},
      "member": {"read": "ALLOW", "write": "CONFIRM", "delete_small": "APPROVE", "dangerous": "DENY"}
    }
  }
  EOF
  ```

- [ ] Enable admission control in `~/.hermes/config.yaml`
  ```yaml
  platforms:
    feishu:
      extra:
        admission_control_enabled: true
  ```

### 2. Directory Permissions

- [ ] Verify directories are writable:
  ```bash
  mkdir -p ~/.hermes/admission
  mkdir -p ~/.hermes/audit
  touch ~/.hermes/admission/.test && rm ~/.hermes/admission/.test
  touch ~/.hermes/audit/.test && rm ~/.hermes/audit/.test
  ```

### 3. Test Configuration

- [ ] Run validation:
  ```bash
  cd ~/.hermes/hermes-agent
  python -c "from gateway.admission import AdmissionController; ctrl = AdmissionController(); valid, errors = ctrl.validate_config(); print('Valid' if valid else f'Errors: {errors}')"
  ```

## Deployment

### 4. Start Gateway

- [ ] Start with admission control enabled:
  ```bash
  cd ~/.hermes/hermes-agent
  python -m gateway.run
  ```

- [ ] Check startup logs for:
  ```
  [admission] Controller initialized (db=..., audit=...)
  [admission] Configuration validated successfully
  [admission] Queue worker started
  [worker] Started fast lane worker
  [worker] Started standard lane worker
  [worker] Started heavy lane worker
  [worker] Started cleanup loop
  ```

### 5. Verify Operation

- [ ] Check queue status:
  ```bash
  python -m gateway.admission.cli status
  ```

- [ ] Check stats:
  ```bash
  python -m gateway.admission.cli stats
  ```

- [ ] Send test message via Feishu and verify:
  - Message appears in logs: `[admission] Admitted user=xxx lane=xxx`
  - Worker processes it: `[worker] Processing xxx from xxx lane`
  - Completion logged: `[worker] Completed xxx in X.XXs`

## Post-Deployment Monitoring

### 6. Monitor Queue Depth

- [ ] Set up periodic checks (every 5 minutes):
  ```bash
  */5 * * * * cd ~/.hermes/hermes-agent && python -m gateway.admission.cli stats >> ~/.hermes/logs/admission-stats.log
  ```

- [ ] Watch for WARNING/CRITICAL in logs:
  ```bash
  tail -f ~/.hermes/logs/gateway.log | grep -E "WARNING|CRITICAL"
  ```

### 7. Monitor Performance

- [ ] Check processing times:
  ```bash
  grep "Completed.*in" ~/.hermes/logs/gateway.log | tail -20
  ```

- [ ] Verify throughput meets expectations (>25 items/sec under load)

### 8. Audit Trail

- [ ] Verify audit logs are being written:
  ```bash
  ls -lh ~/.hermes/audit/
  tail ~/.hermes/audit/$(date +%Y-%m-%d).jsonl | jq .
  ```

## Rollback Plan

### 9. Disable Admission Control

If issues occur:

- [ ] Edit `~/.hermes/config.yaml`:
  ```yaml
  platforms:
    feishu:
      extra:
        admission_control_enabled: false
  ```

- [ ] Restart gateway

- [ ] Verify messages process normally without queuing

### 10. Preserve Data

- [ ] Backup queue state:
  ```bash
  cp ~/.hermes/admission/queue.db ~/.hermes/admission/queue.db.backup
  ```

- [ ] Backup audit logs:
  ```bash
  tar -czf ~/admission-audit-backup-$(date +%Y%m%d).tar.gz ~/.hermes/audit/
  ```

## Troubleshooting

### Queue Stuck

```bash
# Check worker status
grep "Queue worker" ~/.hermes/logs/gateway.log | tail -5

# Check for errors
grep -i error ~/.hermes/logs/gateway.log | grep admission | tail -10

# Clear queue if needed (testing only)
python -m gateway.admission.cli clear
```

### High Queue Depth

```bash
# Check current depth
python -m gateway.admission.cli status

# Identify slow lane
grep "Completed.*in" ~/.hermes/logs/gateway.log | awk '{print $NF}' | sort -n | tail -10

# Consider adjusting lane classification if one lane is consistently slow
```

### Permission Errors

```bash
# Verify user-roles.json is valid JSON
python -c "import json; print(json.load(open('~/.hermes/config/user-roles.json'.replace('~', '$HOME'))))"

# Check user mapping
python -c "from tools.permission_policy import get_user_role_by_id; print(get_user_role_by_id('test_user'))"
```

## Success Criteria

- [ ] Gateway starts without errors
- [ ] Messages are queued and processed
- [ ] Queue depth stays bounded (<20 under normal load)
- [ ] Processing time <5s per message on average
- [ ] No CRITICAL warnings in logs
- [ ] Audit logs are being written
- [ ] Metrics show >95% success rate

## Contacts

- System Owner: [Your Name]
- On-Call: [Contact Info]
- Documentation: `~/.hermes/hermes-agent/gateway/admission/README.md`
