# Template System - Complete Guide

## Overview

The Hermes template system allows you to create reusable task templates with parameters, making it easy to automate recurring workflows.

**Current Status:** Phase 2 Complete (Parameterization + Export/Import + Usage Statistics)

## Features

### Core Capabilities

- **Parameterized Templates**: Use `{{variable}}` placeholders in task descriptions
- **Parameter Validation**: Required/optional parameters with defaults
- **Template Rendering**: Preview templates with actual values before creating tasks
- **Export/Import**: Share templates as portable JSON
- **Usage Statistics**: Track how often templates are used
- **Cron Integration**: Create scheduled tasks from templates
- **Dual Interface**: Full feature parity between Gateway (Feishu) and CLI

### Gateway Commands (Feishu)

```
/templates [query] [--type <type>]
  List all templates, optionally filtered by name or type
  Shows usage statistics (e.g., "使用 15 次")

/template create <task_id> <name>
  Create a template from a successful task
  Automatically extracts {{variable}} placeholders

/template show <template_id>
  View template details including parameters

/template edit <template_id> --name <新名称> --content <新内容>
  Edit template name or content
  Re-extracts parameters when content changes

/template render <template_id> key=value ...
  Preview rendered template with actual values
  Automatically records usage statistics

/template export <template_id>
  Export template as portable JSON

/template import <json>
  Import template from JSON
  Generates new ID and timestamp

/template delete <template_id>
  Delete a template
```

### CLI Commands

```bash
# List templates
hermes template list [query] [--type <type>]

# Create from task
hermes template create <task_id> <name>

# View details
hermes template show <template_id>

# Edit template
hermes template edit <template_id> --name <新名称> --content <新内容>

# Render preview
hermes template render <template_id> --param key=value ...

# Export/Import
hermes template export <template_id>
hermes template import <json>

# Delete
hermes template delete <template_id>

# Create cron job from template
hermes cron create <schedule> --template <template_id> --param key=value
```

## Usage Examples

### Basic Workflow

```bash
# 1. Create a template from a successful task
hermes template create task-abc123 "日报模板"

# 2. View the template
hermes template show abc12345
# Output:
# 📝 日报模板 (abc12345)
# 类型: docs
# 内容: 写 {{date}} 的日报，包含 {{project}} 项目进展
# 参数:
#   - date (required)
#   - project (required)

# 3. Render with actual values
hermes template render abc12345 --param date=2026-04-25 --param project=模板系统
# Output: 写 2026-04-25 的日报，包含 模板系统 项目进展

# 4. Create a cron job
hermes cron create "0 18 * * *" --template abc12345 --param date='$(date +%Y-%m-%d)' --param project=模板系统
```

### Optional Parameters

```bash
# Create template with optional parameter
# Content: "写 {{date}} 的日报{{#project}}，项目：{{project}}{{/project}}"

# Edit to add default
hermes template edit abc12345 --content "写 {{date}} 的日报，项目：{{project}}"
# Then manually set default in params JSON

# Render without optional param (uses default)
hermes template render abc12345 --param date=2026-04-25
```

### Export/Import for Team Sharing

```bash
# User A exports template
hermes template export abc12345 > weekly-report.json

# User B imports template
hermes template import "$(cat weekly-report.json)"
# Output: 导入成功，新模板 ID: def67890
```

### Usage Statistics

Templates automatically track usage when rendered:

```
/templates
# Output:
# 📦 模板列表
# 
# 📝 `abc12345` — 日报模板 (参数: `{{date}}`, `{{project}}`) — 使用 15 次
# 📝 `def67890` — 周报模板 (参数: `{{date}}`) — 使用 8 次
# 📝 `ghi11111` — 新模板 (参数: `{{name}}`)
```

## Parameter System

### Automatic Extraction

Parameters are automatically extracted from `{{variable}}` placeholders:

```
Content: "写 {{date}} 的日报，负责人：{{owner}}"
→ Extracts: {date: {type: "string", required: true}, owner: {type: "string", required: true}}
```

### Parameter Types

Currently supported:
- `string` (default for all extracted parameters)

Future phases may add:
- `number`, `boolean`, `date`, `enum`

### Required vs Optional

- **Required**: Must be provided when rendering
- **Optional**: Can have default values

```python
# Required parameter (default)
{"date": {"type": "string", "required": true}}

# Optional parameter with default
{"project": {"type": "string", "required": false, "default": "默认项目"}}
```

## Export/Import Format

Templates export to portable JSON:

```json
{
  "name": "日报模板",
  "task_type": "docs",
  "request_summary": "写 {{date}} 的日报",
  "params": {
    "date": {
      "type": "string",
      "required": true
    }
  }
}
```

**Excluded fields** (instance-specific):
- `template_id` (regenerated on import)
- `source_task_id` (set to "imported")
- `created_at` (set to import time)
- `usage_count` (reset to 0)
- `last_used_at` (reset to null)

## Database Schema

```sql
CREATE TABLE templates (
    template_id TEXT PRIMARY KEY,
    source_task_id TEXT NOT NULL,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    request_summary TEXT,
    params TEXT,  -- JSON
    created_at REAL NOT NULL,
    usage_count INTEGER DEFAULT 0,
    last_used_at REAL
);

CREATE INDEX idx_templates_created ON templates(created_at DESC);
CREATE INDEX idx_templates_usage ON templates(usage_count DESC);
```

## Migration Safety

The system includes automatic schema migration:

```python
# Checks for missing columns and adds them with defaults
if "params" not in columns:
    conn.execute("ALTER TABLE templates ADD COLUMN params TEXT")
if "usage_count" not in columns:
    conn.execute("ALTER TABLE templates ADD COLUMN usage_count INTEGER DEFAULT 0")
if "last_used_at" not in columns:
    conn.execute("ALTER TABLE templates ADD COLUMN last_used_at REAL")
```

**Backward compatibility:**
- Old code can still read/write without errors
- New columns have safe defaults
- No data loss during migration

## Cron Integration

Templates integrate seamlessly with the cron system:

```bash
# Create scheduled task from template
hermes cron create "0 9 * * 1" --template abc12345 --param date='$(date +%Y-%m-%d)'

# List cron jobs (shows template source)
hermes cron list
# Output:
# ⏰ job-xyz789 — 每周一 9:00
# 模板: 日报模板 (abc12345)
# 参数: date=$(date +%Y-%m-%d)
```

## API Reference

### TemplateStore Methods

```python
# Create
template_id = store.create_from_task(
    source_task_id="task-123",
    name="模板名称",
    task_type="docs",
    request_summary="写 {{date}} 的日报",
    created_at=time.time()
)

# Read
template = store.get(template_id)
templates = store.list_recent(limit=20, sort_by="usage")

# Update
store.edit(template_id, name="新名称", request_summary="新内容")

# Delete
store.delete(template_id)

# Render
rendered = store.render(template_id, {"date": "2026-04-25"})

# Export/Import
data = store.export_template(template_id)
new_id = store.import_template(data)

# Usage tracking
store.record_usage(template_id)  # Called automatically by render()
```

## Testing

Comprehensive test coverage (122 tests, 100% pass rate):

```bash
# Run all template tests
pytest tests/gateway/test_template*.py tests/hermes_cli/test_template*.py -v

# Test categories:
# - CRUD operations (create, read, update, delete)
# - Parameterization (extraction, validation, rendering)
# - Optional parameters with defaults
# - Export/Import (valid/invalid JSON)
# - Usage statistics (tracking, display, sorting)
# - Error handling (not found, missing params)
# - Gateway/CLI parity
# - Cron integration
```

## Future Phases

### Phase 3 (Planned)
- Template versioning (track edit history)
- Template categories/tags
- Template search by content
- Bulk operations (batch export/import)

### Phase 4 (Planned)
- Advanced parameter types (number, date, enum)
- Parameter validation rules (regex, range)
- Conditional rendering (if/else in templates)
- Template inheritance (base templates)

## Troubleshooting

### Template not found
```bash
# Check template ID
hermes template list | grep <name>

# Use full or short ID (first 8 chars)
hermes template show abc12345
```

### Missing required parameter
```bash
# Check required params
hermes template show <template_id>

# Provide all required params
hermes template render <template_id> --param key1=value1 --param key2=value2
```

### Export/Import fails
```bash
# Validate JSON format
cat template.json | jq .

# Check required fields
jq 'has("name") and has("task_type") and has("request_summary")' template.json
```

## Security Considerations

- **SQL Injection**: All queries use parameterized statements
- **Concurrency**: Atomic operations for usage counting
- **Data Validation**: Parameter validation before rendering
- **Access Control**: Templates are user-scoped (future: team sharing)

## Performance

- **Database**: SQLite with indexes on created_at and usage_count
- **Query Performance**: O(1) for get, O(log n) for list (indexed)
- **Concurrency**: Safe for concurrent reads, atomic writes
- **Storage**: ~1KB per template (including params JSON)

## Changelog

### 2026-04-25 - Phase 2 Complete
- ✅ Export/Import functionality
- ✅ Usage statistics tracking
- ✅ Gateway and CLI feature parity
- ✅ 122 tests, 100% pass rate

### 2026-04-24 - Phase 1.5-C
- ✅ Template editing
- ✅ Optional parameters with defaults
- ✅ Cron integration

### 2026-04-23 - Phase 1.5-B
- ✅ Parameter extraction and validation
- ✅ Template rendering

### 2026-04-22 - Phase 1.5-A
- ✅ Basic CRUD operations
- ✅ Gateway and CLI interfaces
