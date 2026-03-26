# Migration: `record_type` → `mode`

Petey now uses `mode: query` (one record per file) and `mode: table` (multiple records per page) instead of `record_type: single` / `record_type: array`. The petey library accepts both for backwards compat, but petey-web should be updated to use the new terminology.

## Mapping

| Old | New |
|-----|-----|
| `record_type: single` | `mode: query` |
| `record_type: array` | `mode: table` |
| `is_array` | `is_table` |

## Files to update

### server/app.py:160-161
```python
# Old
is_array = spec.get("record_type") == "array"
if is_array:

# New
is_table = spec.get("mode") == "table" or spec.get("record_type") == "array"
if is_table:
```

### server/runs.py:64
```python
# Old
"record_type": data.get("record_type", "single"),

# New
"mode": data.get("mode", "query"),
```

### templates/runs.html:296-297
```javascript
// Old
html += '...<label>Record Type</label><span>' + (run.record_type || '—') + '</span></div>';
if (run.record_type === 'array') {

// New
html += '...<label>Mode</label><span>' + (run.mode || '—') + '</span></div>';
if (run.mode === 'table') {
```

### templates/builder.html
Multiple references — search and replace:

| Old | New |
|-----|-----|
| `spec.record_type \|\| 'single'` | `spec.mode \|\| 'query'` |
| `spec.record_type === 'array'` | `spec.mode === 'table'` |
| `spec.record_type = 'array'` | `spec.mode = 'table'` |
| `record_type: ' + spec.record_type` | `mode: ' + spec.mode` |
| `recordType = 'table'` | no change (already correct) |
| `recordType = 'single'` | `recordType = 'query'` |

### templates/template_builder.html
```javascript
// Old
recordType = spec.record_type === 'array' ? 'array' : 'single';
if (recordType === 'array') yaml += 'record_type: array\n';

// New
recordType = spec.mode === 'table' ? 'table' : 'query';
if (recordType === 'table') yaml += 'mode: table\n';
```

### schemas/mci_schema_test.yaml
```yaml
# Old
record_type: array

# New
mode: table
```

### tests/test_extract.py
Replace `"record_type": "array"` with `"mode": "table"` in test specs. Consider keeping one test with `record_type: array` to verify backwards compat still works through petey's `build_model`.

## Notes
- The petey library's `build_model()` in `schema.py` accepts both `mode: table` and `record_type: array` — so existing saved schemas won't break.
- The `is_table` variable name replaces `is_array` everywhere.
- The UI already uses "table" as the toggle label (`recordType = 'table'`), so this just aligns the data model with the UI.
