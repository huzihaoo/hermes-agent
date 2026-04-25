# Template System Changelog

## [Phase 2] - 2026-04-25

### Added
- **Export/Import**: Share templates as portable JSON
  - `hermes template export <id>` exports to JSON
  - `hermes template import <json>` imports from JSON
  - Portable format excludes instance-specific data
  - New IDs generated on import

- **Usage Statistics**: Track template usage
  - `usage_count` field tracks total uses
  - `last_used_at` field tracks last usage time
  - Automatically recorded on `render()`
  - Displayed in template lists ("使用 15 次")
  - Sortable by usage: `list_recent(sort_by="usage")`

- **Database Enhancements**:
  - New columns: `usage_count`, `last_used_at`
  - New index: `idx_templates_usage`
  - Automatic migration with backward compatibility

### Changed
- `list_recent()` now accepts `sort_by` parameter ("created" or "usage")
- Gateway `/templates` command shows usage statistics
- Template rendering now auto-records usage

### Testing
- Added 4 new test files for usage statistics
- Total: 122 tests, 100% pass rate
- Coverage: export/import, usage tracking, sorting

### Documentation
- Complete user guide: `docs/gateway/template-system.md`
- API reference with examples
- Migration safety notes

---

## [Phase 1.5-C] - 2026-04-24

### Added
- Template editing: `hermes template edit <id> --name <name> --content <content>`
- Optional parameters with defaults
- Cron integration: `hermes cron create <schedule> --template <id>`

### Changed
- Parameter extraction now supports optional params
- Template content can be updated (re-extracts params)

### Testing
- Added 3 new test files for editing
- Total: 118 tests

---

## [Phase 1.5-B] - 2026-04-23

### Added
- Parameter extraction from `{{variable}}` placeholders
- Parameter validation (required/optional)
- Template rendering: `hermes template render <id> --param key=value`

### Changed
- `request_summary` now supports parameterization
- Database schema: added `params` column (JSON)

### Testing
- Added 2 new test files for parameterization
- Total: 115 tests

---

## [Phase 1.5-A] - 2026-04-22

### Added
- Basic template CRUD operations
- Gateway commands: `/template create`, `/template show`, `/templates`
- CLI commands: `hermes template create`, `hermes template show`, `hermes template list`
- SQLite storage with indexes

### Database Schema
```sql
CREATE TABLE templates (
    template_id TEXT PRIMARY KEY,
    source_task_id TEXT NOT NULL,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    request_summary TEXT,
    created_at REAL NOT NULL
);
```

### Testing
- Initial test suite: 113 tests
- Gateway and CLI parity tests

---

## Future Roadmap

### Phase 3 (Planned)
- Template versioning (edit history)
- Template categories/tags
- Content search
- Bulk operations

### Phase 4 (Planned)
- Advanced parameter types (number, date, enum)
- Validation rules (regex, range)
- Conditional rendering (if/else)
- Template inheritance
