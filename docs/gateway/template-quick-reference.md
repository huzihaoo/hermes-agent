# Template System - Quick Reference

## Gateway (Feishu) Commands

```
/templates                          # List all templates
/templates 日报                     # Search by name
/templates --type docs              # Filter by type

/template create <task_id> <name>   # Create from task
/template show <id>                 # View details
/template edit <id> --name <新名称>  # Edit name
/template edit <id> --content <新内容> # Edit content
/template render <id> key=val ...   # Preview with values
/template export <id>               # Export as JSON
/template import <json>             # Import from JSON
/template delete <id>               # Delete template
```

## CLI Commands

```bash
hermes template list [query] [--type <type>]
hermes template create <task_id> <name>
hermes template show <id>
hermes template edit <id> --name <name> --content <content>
hermes template render <id> --param key=value ...
hermes template export <id>
hermes template import <json>
hermes template delete <id>
```

## Cron Integration

```bash
# Create scheduled task from template
hermes cron create "0 9 * * 1" --template <id> --param key=value

# List cron jobs (shows template source)
hermes cron list
```

## Parameter Syntax

### In Template Content
```
写 {{date}} 的日报，负责人：{{owner}}
```

### When Rendering
```bash
# Gateway
/template render abc12345 date=2026-04-25 owner=张三

# CLI
hermes template render abc12345 --param date=2026-04-25 --param owner=张三
```

## Export/Import

### Export
```bash
# CLI
hermes template export abc12345 > template.json

# Gateway
/template export abc12345
# Copy JSON from response
```

### Import
```bash
# CLI
hermes template import "$(cat template.json)"

# Gateway
/template import {"name":"模板名","task_type":"docs",...}
```

## Usage Statistics

Templates automatically track usage:
- Every `render()` increments `usage_count`
- Updates `last_used_at` timestamp
- Displayed in lists: "使用 15 次"
- Sortable by usage (future CLI feature)

## Common Workflows

### Daily Report Template
```bash
# 1. Create template
hermes template create task-123 "日报模板"

# 2. Schedule daily at 6pm
hermes cron create "0 18 * * *" \
  --template <id> \
  --param date='$(date +%Y-%m-%d)' \
  --param project=当前项目
```

### Team Template Sharing
```bash
# User A
hermes template export abc12345 > weekly-report.json

# User B
hermes template import "$(cat weekly-report.json)"
```

### Template Editing
```bash
# Update content (re-extracts parameters)
hermes template edit abc12345 \
  --content "写 {{date}} 的日报，项目：{{project}}，进展：{{progress}}"

# Preview changes
hermes template render abc12345 \
  --param date=2026-04-25 \
  --param project=模板系统 \
  --param progress=完成Phase2
```

## Task Types

Common task types:
- `docs` - Documentation tasks
- `coding` - Code implementation
- `research` - Research tasks
- `chat` - Chat/discussion tasks
- `cron` - Scheduled tasks

## Tips

1. **Use descriptive names**: "日报模板" better than "template1"
2. **Keep parameters simple**: Use `{{date}}` not `{{report_submission_date}}`
3. **Test before scheduling**: Use `render` to preview before creating cron jobs
4. **Export for backup**: Regularly export important templates
5. **Check usage stats**: Identify unused templates for cleanup

## Troubleshooting

### "Template not found"
```bash
# List all templates to find correct ID
hermes template list

# Use first 8 chars of ID
hermes template show abc12345  # Not full UUID
```

### "Missing required parameter"
```bash
# Check what parameters are needed
hermes template show <id>

# Provide all required params
hermes template render <id> --param key1=val1 --param key2=val2
```

### "Invalid JSON"
```bash
# Validate JSON before import
cat template.json | jq .

# Check required fields
jq 'has("name") and has("task_type") and has("request_summary")' template.json
```

## Database Location

```
~/.hermes/analytics/templates.db
```

## API (Python)

```python
from gateway.tasks.template import TemplateStore
from pathlib import Path

store = TemplateStore(db_path=Path.home() / ".hermes/analytics/templates.db")

# Create
template_id = store.create_from_task(
    source_task_id="task-123",
    name="模板名称",
    task_type="docs",
    request_summary="写 {{date}} 的日报",
    created_at=time.time()
)

# Render
rendered = store.render(template_id, {"date": "2026-04-25"})

# Export
data = store.export_template(template_id)

# Import
new_id = store.import_template(data)
```

## See Also

- Full documentation: `docs/gateway/template-system.md`
- Changelog: `docs/gateway/template-changelog.md`
- Test suite: `tests/gateway/test_template*.py`
