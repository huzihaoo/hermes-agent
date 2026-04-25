# Domain-Based Queue Upgrade Plan

> **Goal:** Upgrade the current lane-only admission queue to a two-level domain+lane model,
> so that user/group/VM messages are hard-isolated at the domain level, while retaining
> fast/standard/heavy lane parallelism within each domain.

## Current State

### What exists
- `types.py`: `Lane = Literal["fast", "standard", "heavy"]`, `QueueItem` dataclass
- `queue.py`: `AdmissionQueue` with flat `_lanes: Dict[Lane, List[QueueItem]]`
- `controller.py`: `admit()` classifies lane, enqueues, audits
- `worker.py`: 3 lane workers + 1 cleanup loop
- `persistence.py`: SQLite save/load with flat table
- Tests: 45 passing

### What is missing
- No concept of "domain" — all messages share the same 3 lanes
- A group chat message and a private DM compete in the same standard queue
- VM-delivery messages have no dedicated isolation
- No per-domain depth monitoring or per-domain status

## Design

### Domain model

```
Domain = Literal["user", "group", "vm"]
```

Each domain gets its own set of 3 lanes:
```
user:fast    user:standard    user:heavy
group:fast   group:standard   group:heavy
vm:fast      vm:standard      vm:heavy
```

Total: 9 logical queues, 3 domain workers (each handles its own 3 lanes).

### Domain routing rules

| Source signal | Domain |
|---|---|
| Feishu DM (p2p chat) | `user` |
| Feishu group chat | `group` |
| VM delivery / webhook / API call | `vm` |
| Unknown / fallback | `user` |

Routing is determined by `chat_type` + `platform` fields on the incoming message,
not by message content.

### Key: domain_id

Each domain instance has a `domain_id`:
- `user` domain: `domain_id = user_id`
- `group` domain: `domain_id = chat_id`
- `vm` domain: `domain_id = vm_id` or `delivery_id`

This enables per-user and per-group queue isolation and status queries.

### Worker model

Current: 3 lane workers (fast/standard/heavy) + 1 cleanup = 4 tasks

New: 3 domain workers (user/group/vm), each internally round-robins its 3 lanes + 1 cleanup = 4 tasks

This keeps the same task count but adds domain-level isolation.
Within each domain worker, processing order is: fast → standard → heavy (priority within lane).

## Implementation Slices

### Slice 1: Freeze vocabulary (types.py)
- Add `Domain` type
- Add `domain` and `domain_id` fields to `QueueItem`
- Backward compatible: default domain="user", domain_id=user_id

### Slice 2: Upgrade queue (queue.py)
- Change `_lanes` from `Dict[Lane, List]` to `Dict[Domain, Dict[Lane, List]]`
- All existing methods gain optional `domain` parameter
- Backward compatible: methods without domain default to iterating all domains

### Slice 3: Upgrade controller (controller.py)
- Add `_classify_domain()` routing function
- `admit()` resolves domain + domain_id before enqueue
- `get_status()` returns per-domain breakdown
- `format_status_text()` shows domain sections

### Slice 4: Upgrade worker (worker.py)
- Replace 3 lane workers with 3 domain workers
- Each domain worker round-robins fast→standard→heavy within its domain

### Slice 5: Upgrade persistence (persistence.py)
- Add `domain` and `domain_id` columns to SQLite table
- Migration: existing rows get domain="user"

### Slice 6: Tests
- Domain routing tests
- Per-domain isolation tests (group message doesn't block user message)
- Per-domain status visibility
- Persistence migration test

## Out of scope
- Per-domain_id sub-queues (e.g., per-user within user domain) — future P2
- Redis backend — future P2
- Web dashboard — future P3
