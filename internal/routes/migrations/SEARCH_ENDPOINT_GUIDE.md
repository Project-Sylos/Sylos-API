# Search Endpoint - Text/Path Query Guide

## Overview

The search endpoint (`POST /api/migrations/{migrationID}/search`) allows you to search for path review items using various filters. When filtering by **text fields** (specifically `name` and `path`), all queries use **substring matching** with SQL `LIKE '%query_text%'` pattern.

## Text Fields: Name and Path

### Behavior

- **`name` field**: Searches for items where the name (filename or folder name) contains the search text as a substring
- **`path` field**: Searches for items where the full path contains the search text as a substring

Both fields **always use substring matching** (LIKE '%value%'). The `operator` field is ignored for these fields - they always use "contains" behavior.

### Example Requests

#### Search by Name (Always Contains)

For `name` field, the `operator` is optional/ignored - it always uses substring matching:

```json
{
  "conditions": [
    {
      "field": "name",
      "value": "documents"
    }
  ]
}
```

Or with operator (ignored):

```json
{
  "conditions": [
    {
      "field": "name",
      "operator": "contains",
      "value": "documents"
    }
  ]
}
```

This will match:
- ✅ `documents.txt`
- ✅ `my-documents.pdf`
- ✅ `documents_backup`
- ✅ `/folder/documents/subfolder`

#### Search by Path (Always Contains)

For `path` field, the `operator` is optional/ignored - it always uses substring matching:

```json
{
  "conditions": [
    {
      "field": "path",
      "value": "/users/john"
    }
  ]
}
```

This will match:
- ✅ `/users/john/documents/file.txt`
- ✅ `/users/john_old/backup`
- ✅ `/data/users/john/config`

#### Combined Search (Name AND Path)

```json
{
  "conditions": [
    {
      "field": "name",
      "value": "report"
    },
    {
      "field": "path",
      "value": "/2024"
    }
  ]
}
```

This will match items where:
- The name contains "report" **AND**
- The path contains "/2024"

For example:
- ✅ `/documents/2024/annual-report.pdf`
- ✅ `/reports/2024/quarterly-report.xlsx`

## Other Fields (Non-Text)

### Type Field

For the `type` field, use exact match (case-insensitive). The `operator` field is optional/ignored. Acceptable values:
- `"folder"` or `"file"`

```json
{
  "conditions": [
    {
      "field": "type",
      "value": "folder"
    }
  ]
}
```

### TraversalStatus and CopyStatus Fields

For `traversalStatus` and `copyStatus` fields, use exact match (case-insensitive). The `operator` field is optional/ignored.

**TraversalStatus acceptable values:**
- `"pending"`, `"failed"`, `"NotOnSrc"`, `"exclusion_explicit"`, `"exclusion_inherited"`

**CopyStatus acceptable values:**
- Similar status values (pending, failed, successful, etc.)

Note: Matching is case-insensitive, so `"Pending"`, `"PENDING"`, and `"pending"` are all equivalent.

```json
{
  "conditions": [
    {
      "field": "traversalStatus",
      "value": "pending"
    }
  ]
}
```

### Depth and Size Fields (Numeric)

For `depth` and `size` fields, operators are supported:

- `"equals"` or `"="` → exact match (`=`)
- `"gt"` or `">"` → greater than (`>`)
- `"gte"` or `">="` → greater than or equal (`>=`)
- `"lt"` or `"<"` → less than (`<`)
- `"lte"` or `"<="` → less than or equal (`<=`)

```json
{
  "conditions": [
    {
      "field": "depth",
      "operator": "lte",
      "value": 2
    }
  ]
}
```

## Complete Request Examples

### 1. Search for files containing "invoice" in the name

```bash
curl -X POST "http://localhost:8080/api/migrations/migration-123/search?offset=0&limit=1000" \
  -H "Content-Type: application/json" \
  -d '{
    "conditions": [
      {
        "field": "name",
        "value": "invoice"
      }
    ]
  }'
```

### 2. Search for items in a specific path with sorting

```bash
curl -X POST "http://localhost:8080/api/migrations/migration-123/search?offset=0&limit=1000&sortField=name&sortDir=asc" \
  -H "Content-Type: application/json" \
  -d '{
    "conditions": [
      {
        "field": "path",
        "value": "/documents/2024"
      }
    ]
  }'
```

### 3. Search for folders containing "project" in name

```bash
curl -X POST "http://localhost:8080/api/migrations/migration-123/search?offset=0&limit=1000" \
  -H "Content-Type: application/json" \
  -d '{
    "conditions": [
      {
        "field": "name",
        "value": "project"
      },
      {
        "field": "type",
        "value": "folder"
      }
    ]
  }'
```

Note: The `operator` field is optional for `type` field - it always uses exact match (case-insensitive).

### 4. List all items (no conditions)

```bash
curl -X POST "http://localhost:8080/api/migrations/migration-123/search?offset=0&limit=1000" \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Key Points

1. **Name and Path fields**: Always use substring matching - the `operator` field is optional/ignored (always uses "contains" behavior)
2. **Type, TraversalStatus, and CopyStatus fields**: Always use exact match (case-insensitive) - the `operator` field is optional/ignored
3. **Depth and Size fields**: Support operators (`equals`, `gt`, `gte`, `lt`, `lte`) for numeric comparisons
4. **Case sensitivity**: 
   - `name` and `path` fields: case-sensitive substring matching
   - `type`, `traversalStatus`, `copyStatus` fields: case-insensitive exact matching
   - `depth` and `size` fields: numeric comparison (case not applicable)
5. **Multiple conditions**: All conditions are combined with `AND` logic
6. **Path joining**: Results are path-joined, showing both `src` and `dst` nodes when they exist
7. **Statistics**: The response includes `stats` field with aggregated statistics for all matching items (before pagination)
8. **Pagination**: Use `offset` and `limit` query parameters (default: offset=0, limit=1000, max limit=10000)
9. **Sorting**: Use `sortField` and `sortDir` query parameters, or include a `sort` object in the request body

## Response Format

The response follows the same format as the diff endpoint:

```json
{
  "items": {
    "path/to/item": {
      "src": { /* node data */ },
      "dst": { /* node data */ }
    }
  },
  "pagination": {
    "offset": 0,
    "limit": 1000,
    "total": 150,
    "totalFolders": 50,
    "totalFiles": 100,
    "hasMore": false
  },
  "stats": {
    "pendingCount": 100,
    "failedCount": 20,
    "excludedCount": 30,
    "foldersCount": 50,
    "filesCount": 100,
    "foldersRatio": 33.33,
    "filesRatio": 66.67,
    "totalFileSize": {
      "src": 1048576000,
      "dst": 1048576000
    }
  }
}
```

