# Local Mem0 Sidecar Memory Provider

`local_mem0_sidecar` connects Hermes to the local `mem0-gateway` sidecar.
It is separate from `plugins.memory.mem0`, which targets the Mem0 Platform API.

## Sidecar API

Expected endpoints:

- `GET /health`
- `POST /v1/memory/search`
- `POST /v1/memory/list`
- `POST /v1/memory/capture`
- `POST /v1/memory/delete`

## Configuration

Activate explicitly:

```yaml
memory:
  provider: local_mem0_sidecar
```

Provider config lives at `$HERMES_HOME/local_mem0_sidecar.json`:

```json
{
  "base_url": "http://127.0.0.1:8765",
  "api_key": "optional bearer token",
  "user_id": "hermes-user",
  "agent_id": "hermes",
  "default_limit": 5,
  "default_threshold": 0.25,
  "timeout_seconds": 5,
  "auto_capture": false
}
```

Environment overrides:

- `LOCAL_MEM0_SIDECAR_BASE_URL`
- `LOCAL_MEM0_SIDECAR_API_KEY`
- `LOCAL_MEM0_USER_ID`
- `LOCAL_MEM0_AGENT_ID`
- `LOCAL_MEM0_DEFAULT_LIMIT`
- `LOCAL_MEM0_DEFAULT_THRESHOLD`
- `LOCAL_MEM0_TIMEOUT`

## Tools

This provider reuses the standard Mem0 tool names so the agent tool surface stays stable:

- `mem0_profile`
- `mem0_search`
- `mem0_conclude`
- `mem0_promote`

`mem0_conclude` stores explicit facts as scoped candidates with `approval_status=candidate` and `recall_policy=manual_only`.
`mem0_promote` re-captures a reviewed fact with `approval_status=reviewed`, `recall_policy=auto_recall`, and optional `source_candidate_id`; it does not delete the source candidate.

Read-path policy:

- `prefetch` only injects reviewed memories into context.
- `mem0_profile` only returns reviewed memories.
- `mem0_search` defaults to reviewed-only results.
- `mem0_search(include_candidates=true)` can explicitly inspect candidate memories.

## Safety defaults

- Search is scoped to the current user.
- Automatic turn capture is disabled by default.
- Task progress and temporary status should not be stored as memory.
- The provider does not start, stop, or reconfigure the sidecar service.
