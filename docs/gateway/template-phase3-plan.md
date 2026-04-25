# Template System Phase 3 - Planning Document

**Status:** 📋 PLANNING  
**Target Start:** After 2-4 weeks of Phase 2 feedback  
**Estimated Duration:** 2-3 weeks

---

## Phase 2 Recap

**Completed Features:**
- ✅ Export/Import (portable JSON)
- ✅ Usage statistics (usage_count + last_used_at)
- ✅ Gateway and CLI feature parity
- ✅ Automatic schema migration

**Quality Metrics:**
- 122 tests, 100% pass rate
- gstack review: 9.5/10
- 0 SQL injection risks
- 0 critical race conditions

---

## Phase 3 Goals

### Primary Objective
Add **template management** features that make templates easier to organize, discover, and maintain over time.

### Success Criteria
1. Users can organize templates into categories
2. Users can track template evolution over time
3. Users can search templates by content
4. Users can perform bulk operations efficiently

---

## Proposed Features

### Feature 1: Template Versioning

**Problem:** When editing a template, there's no way to undo changes or see what changed.

**Solution:** Track edit history with version snapshots.

**Implementation:**
```sql
CREATE TABLE template_versions (
    version_id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    name TEXT NOT NULL,
    request_summary TEXT,
    params TEXT,
    created_at REAL NOT NULL,
    created_by TEXT,
    change_summary TEXT,
    FOREIGN KEY (template_id) REFERENCES templates(template_id)
);

CREATE INDEX idx_versions_template ON template_versions(template_id, version_number DESC);
```

**API:**
```bash
# View version history
hermes template history <id>

# Revert to a previous version
hermes template revert <id> --version <number>

# Compare versions
hermes template diff <id> --from <v1> --to <v2>
```

**Effort:** Medium (2-3 days)  
**Priority:** High (prevents data loss)

---

### Feature 2: Template Categories/Tags

**Problem:** Hard to organize templates when you have many.

**Solution:** Add category and tag fields.

**Implementation:**
```sql
ALTER TABLE templates ADD COLUMN category TEXT;
ALTER TABLE templates ADD COLUMN tags TEXT; -- JSON array

CREATE INDEX idx_templates_category ON templates(category);
```

**API:**
```bash
# Create with category
hermes template create <task_id> <name> --category "daily-reports"

# Add tags
hermes template tag <id> --add "urgent" "client-facing"

# List by category
hermes template list --category "daily-reports"

# Search by tag
hermes template list --tag "urgent"
```

**Effort:** Low (1-2 days)  
**Priority:** Medium (nice to have)

---

### Feature 3: Content Search

**Problem:** Can't find templates by searching their content.

**Solution:** Add full-text search on request_summary.

**Implementation:**
```sql
-- SQLite FTS5 virtual table
CREATE VIRTUAL TABLE templates_fts USING fts5(
    template_id UNINDEXED,
    name,
    request_summary,
    content=templates,
    content_rowid=rowid
);

-- Triggers to keep FTS in sync
CREATE TRIGGER templates_ai AFTER INSERT ON templates BEGIN
    INSERT INTO templates_fts(rowid, template_id, name, request_summary)
    VALUES (new.rowid, new.template_id, new.name, new.request_summary);
END;

CREATE TRIGGER templates_au AFTER UPDATE ON templates BEGIN
    UPDATE templates_fts SET name = new.name, request_summary = new.request_summary
    WHERE rowid = old.rowid;
END;

CREATE TRIGGER templates_ad AFTER DELETE ON templates BEGIN
    DELETE FROM templates_fts WHERE rowid = old.rowid;
END;
```

**API:**
```bash
# Search by content
hermes template search "日报"

# Search with filters
hermes template search "日报" --category "reports" --tag "daily"
```

**Effort:** Medium (2-3 days)  
**Priority:** High (improves discoverability)

---

### Feature 4: Bulk Operations

**Problem:** No way to export/import multiple templates at once.

**Solution:** Add batch export/import commands.

**Implementation:**
```python
def export_all(self, category: Optional[str] = None, tags: Optional[List[str]] = None) -> List[dict]:
    """Export multiple templates matching filters."""
    templates = self.list_recent(limit=1000)
    if category:
        templates = [t for t in templates if t.get("category") == category]
    if tags:
        templates = [t for t in templates if any(tag in t.get("tags", []) for tag in tags)]
    return [self.export_template(t["template_id"]) for t in templates]

def import_batch(self, templates: List[dict]) -> List[str]:
    """Import multiple templates at once."""
    return [self.import_template(t) for t in templates]
```

**API:**
```bash
# Export all templates
hermes template export-all > all-templates.json

# Export by category
hermes template export-all --category "daily-reports" > daily-reports.json

# Import batch
hermes template import-batch < all-templates.json
```

**Effort:** Low (1 day)  
**Priority:** Medium (convenience feature)

---

## Implementation Plan

### Week 1: Foundation
- [ ] Design database schema changes
- [ ] Write migration scripts
- [ ] Update TemplateStore class
- [ ] Add backward compatibility tests

### Week 2: Core Features
- [ ] Implement template versioning
- [ ] Implement content search (FTS5)
- [ ] Add categories and tags
- [ ] Update Gateway commands

### Week 3: Polish & Testing
- [ ] Implement bulk operations
- [ ] Update CLI commands
- [ ] Write comprehensive tests
- [ ] Update documentation
- [ ] Run gstack review

---

## Database Migration Strategy

### Migration Path: Phase 2 → Phase 3

```python
def migrate_to_phase3(self):
    """Migrate database from Phase 2 to Phase 3."""
    with sqlite3.connect(self.db_path) as conn:
        # Add new columns
        conn.execute("ALTER TABLE templates ADD COLUMN category TEXT")
        conn.execute("ALTER TABLE templates ADD COLUMN tags TEXT")
        
        # Create version tracking table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS template_versions (
                version_id TEXT PRIMARY KEY,
                template_id TEXT NOT NULL,
                version_number INTEGER NOT NULL,
                name TEXT NOT NULL,
                request_summary TEXT,
                params TEXT,
                created_at REAL NOT NULL,
                created_by TEXT,
                change_summary TEXT,
                FOREIGN KEY (template_id) REFERENCES templates(template_id)
            )
        """)
        
        # Create FTS5 table
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS templates_fts USING fts5(
                template_id UNINDEXED,
                name,
                request_summary,
                content=templates,
                content_rowid=rowid
            )
        """)
        
        # Create triggers
        # (see Feature 3 implementation above)
        
        # Create initial versions for existing templates
        conn.execute("""
            INSERT INTO template_versions (
                version_id, template_id, version_number,
                name, request_summary, params, created_at, change_summary
            )
            SELECT 
                hex(randomblob(16)),
                template_id,
                1,
                name,
                request_summary,
                params,
                created_at,
                'Initial version (migrated from Phase 2)'
            FROM templates
        """)
        
        conn.commit()
```

**Rollback Plan:**
- New columns have NULL defaults (safe)
- New tables are independent (can be dropped)
- FTS5 is a virtual table (no data loss if dropped)

---

## Testing Strategy

### Unit Tests
- [ ] Version creation and retrieval
- [ ] Version comparison and diff
- [ ] Category filtering
- [ ] Tag operations (add, remove, search)
- [ ] FTS5 search accuracy
- [ ] Bulk export/import

### Integration Tests
- [ ] Gateway commands with new features
- [ ] CLI commands with new features
- [ ] Migration from Phase 2 to Phase 3
- [ ] Backward compatibility

### Performance Tests
- [ ] FTS5 search performance (1000+ templates)
- [ ] Version history retrieval (100+ versions)
- [ ] Bulk operations (100+ templates)

**Target:** 150+ tests, 100% pass rate

---

## Documentation Updates

### New Documents
- [ ] `template-versioning-guide.md` - How to use version control
- [ ] `template-organization-guide.md` - Categories and tags best practices
- [ ] `template-search-guide.md` - Search syntax and tips

### Updated Documents
- [ ] `template-system.md` - Add Phase 3 features
- [ ] `template-changelog.md` - Add Phase 3 entry
- [ ] `template-quick-reference.md` - Add new commands

---

## Risk Assessment

### Technical Risks

**Risk 1: FTS5 Performance**
- **Impact:** High (affects search speed)
- **Likelihood:** Low (FTS5 is well-optimized)
- **Mitigation:** Benchmark with 10,000+ templates before release

**Risk 2: Version Storage Growth**
- **Impact:** Medium (disk space)
- **Likelihood:** Medium (depends on edit frequency)
- **Mitigation:** Add version pruning (keep last N versions)

**Risk 3: Migration Failures**
- **Impact:** High (data loss)
- **Likelihood:** Low (tested migration)
- **Mitigation:** Backup database before migration, rollback plan

### User Experience Risks

**Risk 1: Feature Complexity**
- **Impact:** Medium (learning curve)
- **Likelihood:** Medium (more features = more complexity)
- **Mitigation:** Good documentation, sensible defaults

**Risk 2: Breaking Changes**
- **Impact:** High (user frustration)
- **Likelihood:** Low (backward compatible)
- **Mitigation:** Maintain API compatibility, deprecation warnings

---

## Success Metrics

### Adoption Metrics
- 50%+ of active users try at least one Phase 3 feature
- 20%+ of templates use categories or tags
- 10%+ of users use version history

### Quality Metrics
- 150+ tests, 100% pass rate
- gstack review score ≥ 9.0/10
- 0 critical bugs in first week
- < 5 minor bugs in first month

### Performance Metrics
- Search response time < 100ms (1000 templates)
- Version retrieval < 50ms
- Bulk export/import < 1s per 100 templates

---

## Go/No-Go Decision

**Phase 3 will proceed if:**
1. ✅ Phase 2 has been stable for 2+ weeks
2. ✅ User feedback is positive (no critical issues)
3. ✅ At least 5 users request Phase 3 features
4. ✅ No major bugs in Phase 2

**Phase 3 will be delayed if:**
- Critical bugs found in Phase 2
- Major refactoring needed
- User feedback suggests different priorities

**Decision Date:** 2026-05-09 (2 weeks after Phase 2 deployment)

---

## Alternative Approaches

### Alternative 1: Skip Versioning, Focus on Search
**Pros:** Faster delivery, simpler implementation  
**Cons:** No undo capability, data loss risk  
**Decision:** Not recommended (versioning is high priority)

### Alternative 2: Use Git for Version Control
**Pros:** Mature version control, familiar to developers  
**Cons:** Complex setup, not user-friendly for non-developers  
**Decision:** Not recommended (too complex for this use case)

### Alternative 3: External Search Service (Elasticsearch)
**Pros:** More powerful search, better performance  
**Cons:** Additional dependency, complex setup  
**Decision:** Not recommended (SQLite FTS5 is sufficient)

---

## Open Questions

1. **Version Retention:** How many versions should we keep per template?
   - Option A: Keep all versions (simple, but grows unbounded)
   - Option B: Keep last 10 versions (bounded, but may lose history)
   - Option C: Keep versions from last 30 days (time-based pruning)
   - **Recommendation:** Start with Option A, add pruning in Phase 4 if needed

2. **Category Hierarchy:** Should categories support nesting?
   - Option A: Flat categories (simple)
   - Option B: Hierarchical categories (flexible, but complex)
   - **Recommendation:** Start with flat, add hierarchy in Phase 4 if needed

3. **Tag Autocomplete:** Should we suggest existing tags when adding new ones?
   - **Recommendation:** Yes, implement in CLI and Gateway

4. **Search Ranking:** How should search results be ranked?
   - Option A: Relevance only (FTS5 default)
   - Option B: Relevance + usage statistics
   - **Recommendation:** Option B (boost frequently-used templates)

---

## Next Steps

1. **Collect Phase 2 feedback** (2-4 weeks)
2. **Review this plan** with stakeholders
3. **Make go/no-go decision** (2026-05-09)
4. **Start implementation** if approved

---

**Document Owner:** 胡子豪  
**Last Updated:** 2026-04-25  
**Status:** Draft (awaiting feedback)
