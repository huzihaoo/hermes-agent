# Template System Phase 2 - User Feedback Collection

**Deployment Date:** 2026-04-25  
**Status:** ✅ LIVE in Production  
**Version:** Phase 2 (Export/Import + Usage Statistics)

---

## How to Provide Feedback

### Via Feishu (飞书)
Send feedback to the Hermes bot using:
```
/feedback template 你的反馈内容
```

### Via GitHub Issues
Create an issue at: https://github.com/NousResearch/hermes-agent/issues
- Label: `feature: templates`
- Title format: `[Template Feedback] Your feedback title`

### Via This Document
Add your feedback below in the appropriate section.

---

## Feedback Categories

### 1. Feature Requests

**What new features would you like to see?**

- [ ] Template versioning (track edit history)
- [ ] Template categories/tags for organization
- [ ] Template search by content
- [ ] Bulk export/import operations
- [ ] Advanced parameter types (number, date, enum)
- [ ] Parameter validation rules (regex, range)
- [ ] Conditional rendering (if/else in templates)
- [ ] Template inheritance (base templates)
- [ ] Template sharing with team members
- [ ] Template marketplace/library

**Other ideas:**
```
(Add your feature requests here)
```

---

### 2. Usability Issues

**What's confusing or hard to use?**

```
(Describe any usability problems you encountered)

Example:
- "The parameter syntax {{variable}} is not intuitive"
- "I don't understand the difference between export and render"
- "The CLI commands are too verbose"
```

---

### 3. Bugs & Issues

**What's broken or not working as expected?**

```
(Report any bugs you found)

Please include:
- What you tried to do
- What happened
- What you expected to happen
- Error messages (if any)
```

---

### 4. Performance Feedback

**Is the system fast enough?**

```
(Report any performance issues)

Example:
- "Template list takes too long to load"
- "Rendering large templates is slow"
- "Export/import feels sluggish"
```

---

### 5. Documentation Feedback

**Is the documentation clear and helpful?**

```
(Feedback on docs/gateway/template-*.md)

Example:
- "Missing examples for X"
- "Section Y is confusing"
- "Need more troubleshooting tips"
```

---

### 6. Success Stories

**What worked well? What do you love?**

```
(Share your positive experiences)

Example:
- "Export/import made it easy to share templates with my team"
- "Usage statistics help me identify my most-used workflows"
- "The CLI is fast and intuitive"
```

---

## Feedback Summary (Updated Weekly)

### Week 1 (2026-04-25 to 2026-05-01)

**Total Feedback:** 0  
**Feature Requests:** 0  
**Bugs Reported:** 0  
**Success Stories:** 0

**Top Requests:**
- (None yet)

**Critical Issues:**
- (None yet)

**Action Items:**
- [ ] Monitor usage statistics
- [ ] Check for error logs
- [ ] Reach out to early adopters

---

### Week 2 (2026-05-02 to 2026-05-08)

(To be filled)

---

## Internal Metrics (Auto-Updated)

### Usage Statistics

```bash
# Run this command to get current stats:
sqlite3 ~/.hermes/analytics/templates.db "
  SELECT 
    COUNT(*) as total_templates,
    SUM(usage_count) as total_uses,
    AVG(usage_count) as avg_uses_per_template,
    MAX(usage_count) as max_uses
  FROM templates
"
```

**Current Stats:**
- Total templates: (TBD)
- Total uses: (TBD)
- Average uses per template: (TBD)
- Most used template: (TBD)

### Error Logs

```bash
# Check for template-related errors:
grep -i "template" ~/.hermes/logs/gateway.log | grep -i "error" | tail -20
```

**Recent Errors:**
- (None yet)

---

## Phase 3 Planning

Based on user feedback, we will prioritize features for Phase 3.

**Current Candidates:**
1. Template versioning (edit history)
2. Template categories/tags
3. Content search
4. Bulk operations

**Decision Criteria:**
- User demand (number of requests)
- Implementation complexity
- Impact on existing features
- Alignment with roadmap

**Target Date:** TBD (after 2-4 weeks of feedback collection)

---

## Contact

**Maintainer:** 胡子豪  
**Questions:** Ask via Feishu bot or create a GitHub issue

---

**Last Updated:** 2026-04-25
