# Feishu Topic Data File Reading Implementation Plan

> For Hermes: use subagent-driven-development if executing this plan task-by-task.

Goal: make the Feishu bot reliably read ordinary files posted inside a Feishu topic/thread, expose the downloaded local file path to the agent/VM task flow, and fail clearly for unsupported folder/oversized cases.

Architecture: extend the existing Feishu adapter rather than adding a separate bot. The adapter already parses `file_key`, downloads IM message resources with `GetMessageResourceRequest`, caches files under Hermes document cache, and passes local paths through `MessageEvent.media_urls`. The missing landing work is: stricter topic/file coverage, folder detection with Drive fallback, explicit error surfacing, size/type policy, and regression/live-adjacent tests.

Tech stack: Python Hermes gateway, lark_oapi IM v1 message/resource APIs, optional Feishu Drive APIs for folder/file-token support, existing gateway/platforms/base.py document cache, existing gateway/run.py document context injection, pytest.

Current context / implementation truth

- Repo: `/Users/songying/.hermes/hermes-agent`
- Branch observed: `overlay/stable`
- Important existing files:
  - `gateway/platforms/feishu.py`
  - `gateway/platforms/base.py`
  - `gateway/run.py`
  - `tests/gateway/test_feishu.py`
- Existing code already has these pieces:
  - Imports `GetMessageResourceRequest` in `gateway/platforms/feishu.py`.
  - `normalize_feishu_message()` parses `file`, `audio`, `media`, `image`, and rich-text post embedded file/image refs into `media_refs` / `image_keys`.
  - `_extract_message_content()` calls `_download_feishu_message_resources()` and returns `media_urls`, `media_types`.
  - `_download_feishu_message_resource()` calls `self._client.im.v1.message_resource.get(...)` and caches returned bytes via `cache_document_from_bytes()`.
  - `MessageEvent.media_urls` are later consumed by `gateway/run.py::_preprocess_message_for_agent(...)` so document files become local path notes for the agent.
- Known limitation from validated Feishu behavior:
  - `msg_type=folder` is not downloadable through IM message resource. `type=folder` returns `234001`; `type=file` returns `234003`.
  - Feishu Drive folder listing needs Drive scopes. Without them, API returns access denied.
  - Large zip/file downloads may return `234037 Downloaded file size exceeds limit`; this should fail clearly and ask for NAS/VM path rather than pretending no file exists.

In scope

1. Ordinary topic file messages (`msg_type=file`) are downloaded and cached.
2. Rich-text topic posts with embedded file resources are downloaded and cached.
3. Agent receives explicit local file paths and filenames, including binary/data files like `.dbc`, `.zip`, `.mcap`, `.json`, `.csv`.
4. Unsupported folder messages produce a deterministic user-visible explanation.
5. Oversized file/resource download failures produce a deterministic user-visible explanation.
6. Optional Drive folder/file-token path is designed behind a capability flag and scopes check; no silent retry loops.
7. Tests cover parser, downloader, failure modes, and agent context injection.

Out of scope for first slice

- Full recursive Drive folder sync for every possible Feishu Drive node type.
- Automatically decompressing or processing arbitrary zip/data packages at gateway layer.
- Sending files to Feishu; outbound upload is already separate.
- Direct VM business execution. VM processing should still go through shared-state / vm_task_submit.

Proposed behavior

A. Ordinary file in topic

Input:
- Feishu event has `message_type=file`
- content has `file_key` and `file_name`
- source has topic/thread metadata

Gateway behavior:
1. Normalize to `FeishuNormalizedMessage(preferred_message_type='document', media_refs=[...])`.
2. Download with IM resource API:
   `im.v1.message_resource.get(GetMessageResourceRequest(message_id, file_key, type='file'))`
3. Cache bytes with original filename preserved.
4. Create `MessageEvent` with:
   - `message_type=DOCUMENT`
   - `media_urls=[cached_path]`
   - `media_types=[content_type or guessed type]`
   - `text` either caption/text or empty
5. `gateway/run.py` prepends document context:
   `[The user sent a document: 'xxx'. The file is saved at: <path>. ...]`

B. Folder in topic

Input:
- `message_type=folder` or content indicates folder node

First-slice behavior:
- Do not call IM resource API repeatedly.
- Produce a clear note to the agent/user:
  `飞书文件夹不能通过消息附件接口直接下载；请发 zip 或提供 VM/NAS 路径。若要支持文件夹读取，需要开通 Drive scopes 并走 Drive API。`

Second-slice behavior with Drive scopes:
- Detect Drive capabilities at startup or on first folder event.
- Resolve folder token from message content if available.
- Use Drive Explorer/list API to list children.
- Download supported child files to a task-specific cache dir.
- Enforce count/size/depth limits.
- Expose a manifest file path to the agent.

C. Oversized normal file

If IM resource API returns code `234037` or equivalent message:
- Do not silently return empty media.
- Attach a warning to normalized event text or message metadata so the agent can tell user:
  `文件超过飞书机器人消息下载限制，请提供 VM/NAS 路径或拆分/压缩为较小文件。`

File-by-file implementation plan

Task 1: Add explicit folder normalization

Objective: recognize Feishu `folder` messages and keep enough metadata to explain why they cannot be downloaded.

Files:
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu.py`

Steps:
1. Extend `FeishuNormalizedMessage` with optional warnings/unsupported refs, or use existing `metadata` field if already present in the dataclass.
2. In `normalize_feishu_message()`, add branch for `normalized_type == 'folder'`:
   - parse `file_key`, `file_name`
   - set `text_content` to a short placeholder like `[Folder: <name>]`
   - set `preferred_message_type='document'` only if downstream needs document handling; otherwise keep text to avoid fake download
   - set metadata warning: `unsupported_feishu_folder`
3. Add test:
   - `test_extract_folder_message_reports_unsupported_without_download`
   - Mock `_download_feishu_message_resource` and assert it is not called.
   - Assert text includes folder name and unsupported explanation.

Task 2: Preserve downloadable file errors as user-visible notes

Objective: when resource download fails for known Feishu limitation codes, the agent gets a clear reason instead of just no attachment.

Files:
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu.py`

Steps:
1. Introduce small dataclass or helper result internally:
   - `FeishuResourceDownloadResult(path: str, media_type: str, warning: str = '')`
   Keep public method compatibility if needed by wrapping old tuple return.
2. In `_download_feishu_message_resource()`, identify response codes:
   - `234037`: oversized file
   - `234001`, `234003`: invalid folder/file mismatch
   - `99991672`: Drive permission/access denied if encountered by future Drive path
3. Accumulate warnings in `_download_feishu_message_resources()`.
4. Append warnings to `text` in `_extract_message_content()` in a concise way.
5. Add tests for oversized response using fake response object with `.success() == False`, `.code == 234037`.

Task 3: Verify ordinary file path remains end-to-end visible

Objective: ensure regular files posted in topic are not only downloaded but also shown to agent as saved paths.

Files:
- Modify tests only initially:
  - `tests/gateway/test_feishu.py`
  - possibly `tests/gateway/test_run_*.py` or add focused test for `_preprocess_message_for_agent`

Steps:
1. Keep existing `test_extract_file_message_downloads_and_caches`.
2. Add/extend test for `gateway/run.py::_preprocess_message_for_agent()` with a `MessageEvent(message_type=DOCUMENT, media_urls=[...])`.
3. Assert resulting `message_text` includes:
   - original display filename
   - exact cached path
   - for text docs, included content when applicable
4. If display-name parsing is too dependent on cache filename prefix, add helper to preserve original filename more explicitly.

Task 4: Add Feishu file intake policy limits

Objective: avoid gateway memory blowups or surprise downloads.

Files:
- Modify: `gateway/platforms/feishu.py`
- Possibly config docs: `README.md` or Hermes config docs if present
- Test: `tests/gateway/test_feishu.py`

Policy defaults:
- `HERMES_FEISHU_MAX_FILE_BYTES`: default `32MB`. `0` disables host downloads entirely. Because the current Lark SDK path can materialize response bytes in host memory, do not raise this casually; medium/large files must land in VM `/mnt/tmp/<task_id>/...` with user-visible CIFS path `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/...`.
- No v1 extension blocklist. Reading a file is not the dangerous operation; execution remains governed by normal tool/security policy. Keep `.dbc`, `.zip`, `.mcap`, `.csv`, `.json`, and similar PNC data files eligible when under the small-file cap.
- True Feishu folder traversal is out of v1. Folder messages fail clearly and instruct users to send a zip or provide VM/NAS path; Drive folder support requires a separate scoped v2 with Drive permissions and VM landing.

Steps:
1. Add helper for configured host byte cap and fail-closed warning text.
2. Check the cap while reading bytes; if exceeded, do not cache or pass a document path to the agent.
3. Return a clear VM/NAS handoff warning for blocked-by-size files.
4. Tests for allowed small `.dbc`/ordinary files, `HERMES_FEISHU_MAX_FILE_BYTES=0` kill switch, local oversize, and Feishu `234037`.

Task 5: Optional Drive folder support behind capability flag

Objective: support true Feishu folder reading only when app scopes are granted; fail cleanly otherwise.

Files:
- Modify: `gateway/platforms/feishu.py`
- Add tests: `tests/gateway/test_feishu_drive_resources.py` or same `test_feishu.py`
- Update ops docs / knowledge after implementation

Required Feishu app permissions to confirm in open platform:
- IM message receive + resource download permissions already needed for ordinary file intake.
- Drive read permissions such as `drive:drive` or `drive:drive.metadata:readonly` for folder listing; exact app scopes should be verified against current Feishu console/API because names vary by tenant/app type.

Design:
1. Add config/env flag:
   - `HERMES_FEISHU_ENABLE_DRIVE_FOLDER_DOWNLOAD=1`
   - default off
2. Add adapter method `_download_feishu_folder_resource(message_id, folder_key, folder_name)`.
3. First call a Drive list API for folder children.
4. Download child files only if:
   - total count <= configured max
   - total bytes <= configured max
   - depth <= configured max, default 1 for first release
5. Cache into a manifest directory under Hermes cache, e.g. `cache/documents/<timestamp>_<folder>/`.
6. Return a manifest `.json` or `.md` path as `MessageEvent.media_urls` so agent sees a single document listing all downloaded files.
7. If Drive API returns access denied, append actionable warning and stop.

Task 6: Live-adjacent smoke without sending messages

Objective: prove the downloader path works against mocked Lark SDK shapes and current code, without hitting live Feishu.

Files:
- Test: `tests/gateway/test_feishu.py`

Steps:
1. Build fake `message_resource.get` that returns:
   - success file with `.file` BytesIO, headers content-type, optional `file_name`
   - oversized failure with code 234037
   - folder invalid failure with 234001/234003
2. Run focused tests:
   `cd /Users/songying/.hermes/hermes-agent && python -m pytest tests/gateway/test_feishu.py -q`
3. Run py_compile:
   `python -m py_compile gateway/platforms/feishu.py gateway/run.py tests/gateway/test_feishu.py`

Task 7: Real Feishu smoke, read-only/download-only

Objective: verify against real Feishu API using a controlled topic file.

Preconditions:
- Bot is in the test group/topic.
- Test file is small ordinary file, e.g. `hello.txt` and one PNC-like `.dbc` sample.
- No external reply/send is required for this smoke unless separately approved.

Steps:
1. Send test file in bot-visible topic.
2. Locate inbound log line with `Received raw message type=file message_id=...`.
3. Confirm cache path was logged as `Cached message document resource at ...`.
4. Confirm agent prompt includes saved path.
5. Try folder message and confirm clear unsupported warning.
6. Try large zip only if safe and known; otherwise skip destructive/expensive large-file test.

Verification commands

Focused:
- `cd /Users/songying/.hermes/hermes-agent`
- `python -m pytest tests/gateway/test_feishu.py -q`
- `python -m py_compile gateway/platforms/feishu.py gateway/run.py tests/gateway/test_feishu.py`

Broader if routing was touched:
- `python -m pytest tests/gateway/test_feishu.py tests/tools/test_send_message_tool.py -q`
- If VM task routing or notifier changed, also run shared-state tests from the routing skill; but this file-intake plan should avoid touching routing unless needed.

Operational rollout

1. Implement ordinary file/folder-warning/oversize-warning first; do not wait for Drive folder support.
2. Restart gateway only after tests pass; gateway restart is a service action and should be explicitly scoped.
3. Run controlled topic smoke with small file.
4. Update knowledge/runbook with exact Feishu app scopes and observed errors.
5. Only then enable Drive folder support in a second release if the app owner approves Drive scopes.

Risks / tradeoffs

- Lark SDK response for message resources may load full file into memory. For very large files this can be unsafe. Enforce size limits where possible and rely on Feishu error codes where pre-size is unavailable.
- Drive folder support requires broader app permissions. This has privacy/security impact; default should be off and scoped to bot-visible files/folders.
- Downloading arbitrary files from group topics expands data exposure. Cache TTL/cleanup should remain active; avoid storing data permanently unless a VM task explicitly needs it.
- Folder recursion can explode in file count/size. Limit depth/count/bytes and produce manifest.
- Existing repo has unrelated local modifications. Implementation should use path-scoped diffs and avoid touching unrelated files.

Recommended revised v1 from gstack review

1. Ordinary small Feishu file intake stays in host gateway only up to a strict small-file limit.
2. All medium/large data-file handling must land in VM temp storage under `/mnt/tmp/<task_id>/...` and user-visible CIFS path `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/...`.
3. Gateway must not parse or decompress large files locally. It only does metadata detection, small-file download when safe, or asks for/proxies to a VM/NAS path.
4. For any parsing/conversion task, submit VM work via shared-state / `vm_task_submit`; parsing memory pressure belongs to VM worker processes using `/mnt/tmp`, not host gateway memory.
5. Drive folder sync remains v2 and must also land into `/mnt/tmp/<task_id>/...`, never Hermes host cache.

Do these first:
1. Add folder normalization + unsupported warning.
2. Add known error-code warnings for `_download_feishu_message_resource()`.
3. Add strict host download size gate. Default target: 32MB or lower unless explicitly configured.
4. Add tests for ordinary small file, folder, oversized file, and VM/NAS path handoff wording.
5. Verify agent context injection for document saved path.

This gives immediate value: ordinary small data files in topics become usable, and anything likely to create memory pressure is routed to `/mnt/tmp`/VM instead of silently stressing the host gateway.
