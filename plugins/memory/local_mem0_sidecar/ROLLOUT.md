# Local Mem0 Sidecar Rollout Notes

## Scope

This change adds the `local_mem0_sidecar` memory provider and wires it into `hermes doctor` and `hermes status` observability. It is intentionally separate from the cloud Mem0 provider.

## Live rollout performed on this host

- `~/.hermes/config.yaml`
  - `memory.provider: local_mem0_sidecar`
- `~/.hermes/local_mem0_sidecar.json`
  - `base_url: http://127.0.0.1:8765`
  - `api_key: <configured, not committed>`
  - `auto_capture: false`
- `~/.hermes/runtime/mem0-gateway/config.json`
  - `bind: 127.0.0.1`
- Restarted LaunchAgents:
  - `ai.mem0.gateway`
  - `ai.hermes.gateway`

## Verification commands

```bash
python -m pytest -q -o addopts='' \
  tests/plugins/memory/test_local_mem0_sidecar.py \
  tests/hermes_cli/test_status.py \
  tests/hermes_cli/test_status_model_provider.py \
  tests/hermes_cli/test_memory_setup.py \
  tests/hermes_cli/test_doctor_local_mem0_sidecar.py \
  tests/run_agent/test_memory_provider_init.py \
  tests/agent/test_memory_provider.py

python3 -m py_compile \
  hermes_cli/status.py \
  hermes_cli/doctor.py \
  plugins/memory/local_mem0_sidecar/__init__.py \
  tests/hermes_cli/test_status.py \
  tests/hermes_cli/test_memory_setup.py \
  tests/hermes_cli/test_doctor_local_mem0_sidecar.py \
  tests/plugins/memory/test_local_mem0_sidecar.py

git diff --check
```

Expected status output includes:

```text
Memory:      local_mem0_sidecar (configured)
Memory Policy: search-only, candidate-gated, reviewed-auto-recall
Memory URL:  http://127.0.0.1:8765
Memory Health: ok
```

## Rollback

To return to built-in memory:

1. Set `memory.provider` back to an empty string in `~/.hermes/config.yaml`.
2. Restart Hermes Gateway:

```bash
launchctl kickstart -k gui/$(id -u)/ai.hermes.gateway
```

To revert sidecar binding only:

1. Set `bind` in `~/.hermes/runtime/mem0-gateway/config.json` back to the prior value if needed.
2. Restart the sidecar:

```bash
launchctl kickstart -k gui/$(id -u)/ai.mem0.gateway
```

Keep `~/.hermes/local_mem0_sidecar.json` mode `600` because it contains the local sidecar API key.
