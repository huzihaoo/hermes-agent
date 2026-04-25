# Approval Timeout & Expired Card Implementation

## Overview
Implemented approval timeout callback mechanism and Feishu expired card updates for better UX when dangerous command approvals time out.

## Changes

### 1. Core Timeout Callback Infrastructure (`tools/approval.py`)
- Already had `register_gateway_timeout_callback()` at line 246
- Callback signature: `cb(choice: str)` where choice is "timeout"
- Invoked when approval times out in `check_all_command_guards()`

### 2. Feishu Platform Adapter (`gateway/platforms/feishu.py`)

#### New State
- `_approval_callbacks: Dict[str, Any]` — session_key → callback mapping

#### New Methods
- `register_approval_timeout_callback(session_key, callback)` — public API for registering timeout callbacks
- `_build_expired_approval_card()` — static method returning grey card with "⏱️ Approval Expired" header
- `_update_approval_card_to_expired(message_id, chat_id)` — async method using message PATCH API to update card

#### Enhanced Methods
- `_gc_stale_approval_state()` — now fires timeout callbacks and schedules card updates via `run_coroutine_threadsafe`
- `_resolve_approval()` — cleans up timeout callback on normal resolution to prevent leaks

### 3. Gateway Integration (`gateway/run.py`)

#### Imports
- Added `register_gateway_timeout_callback` to approval imports

#### Callbacks
- `_approval_timeout_sync(choice)` — bridges sync→async for Feishu card updates
- Registered via `register_gateway_timeout_callback(_approval_session_key, _approval_timeout_sync)`
- Also calls `_status_adapter.register_approval_timeout_callback()` for Feishu-specific handling

#### Degradation Warning
- Enhanced fallback logging when `send_exec_approval` fails
- Feishu-specific warning: "Interactive approval card failed (API error or missing permissions)"

### 4. Tests

#### New Test File: `tests/gateway/test_feishu_approval_timeout.py` (7 tests)
- `test_register_timeout_callback` — callback registration
- `test_timeout_callback_invoked_on_gc` — GC triggers callback
- `test_timeout_callback_cleaned_on_normal_resolve` — cleanup on normal resolution
- `test_builds_expired_card` — expired card structure
- `test_updates_card_to_expired` — card update success
- `test_handles_update_failure_gracefully` — error handling
- `test_skips_update_when_not_connected` — disconnected state

#### Fixed Test: `tests/gateway/test_feishu_approval_buttons.py`
- `test_returns_card_for_approve_action` — added `get_user_role_by_id` mock for permission check

#### Test Results
- **147 tests passed** (18 Feishu approval + 7 new timeout + 3 approval timeout card + 119 tools/approval)
- 0 failures, 0 errors

## User Experience Flow

### Before Timeout
1. User triggers dangerous command
2. Feishu sends interactive card with 4 buttons (Allow Once/Session/Always, Deny)
3. Approval state stored with `created_at` timestamp

### On Timeout (default 5 minutes)
1. `_gc_stale_approval_state()` detects stale approval (age > TTL)
2. Fires registered timeout callback with choice="timeout"
3. Schedules `_update_approval_card_to_expired()` on event loop
4. Card updates to grey header: "⏱️ Approval Expired"
5. Buttons removed, shows "This approval request has expired and is no longer valid"
6. Agent receives "timed out" response and denies command

### On Normal Resolution
1. User clicks button (e.g., "Allow Once")
2. `_resolve_approval()` pops state and cleans up timeout callback
3. Card updates to green/red based on choice
4. Agent unblocks and proceeds

## Technical Details

### Thread Safety
- `_approval_callbacks` accessed from both agent threads (registration) and GC thread (timeout)
- No explicit locking needed — dict operations are atomic in CPython
- Callback cleanup uses `.pop()` which is thread-safe

### Event Loop Handling
- GC runs in sync context but needs to schedule async card update
- Uses `asyncio.run_coroutine_threadsafe(coro, self._loop)` instead of `create_task`
- Guards with `self._loop and not self._loop.is_closed()` check

### Error Handling
- Timeout callback exceptions logged but don't crash GC
- Card update failures logged as warnings
- Disconnected state skips card update silently

## Configuration
- Timeout TTL: `_FEISHU_APPROVAL_STATE_TTL_SECONDS = 10 * 60` (10 minutes)
- Approval timeout: configured via `tools.approval._get_approval_config()` (default 5 minutes)
- GC runs on every `send_exec_approval()` call

## Files Modified
- `gateway/platforms/feishu.py` — core implementation
- `gateway/run.py` — integration and degradation warning
- `tests/gateway/test_feishu_approval_buttons.py` — permission mock fix
- `tests/gateway/test_feishu_approval_timeout.py` — new test file

## Files Deleted
- `gateway/run_approval_timeout_patch.py` — patch applied and removed
