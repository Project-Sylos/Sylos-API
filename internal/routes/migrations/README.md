# Migration API Endpoints

Complete reference for all migration orchestration endpoints. All endpoints are prefixed with `/api` in production.

---

## Table of Contents

1. [Migration Setup](#migration-setup)
2. [Migration Lifecycle](#migration-lifecycle)
3. [Migration Status & Monitoring](#migration-status--monitoring)
4. [Database Management](#database-management)
5. [Logging](#logging)

---

## Migration Setup

### Set Migration Root

Set the source or destination root folder for a migration.

**Endpoint:** `POST /migrations/roots`

**Request Body:**
```json
{
  "migrationId": "d4ob1s530fei623t8sdg",
  "role": "source",
  "serviceId": "local",
  "connectionId": "conn-123",
  "root": {
    "id": "folder-id-123",
    "parentId": "parent-id",
    "parentPath": "/path/to/parent",
    "displayName": "My Folder",
    "locationPath": "/path/to/folder",
    "lastUpdated": "2025-01-15T10:30:45Z",
    "depthLevel": 0,
    "type": "folder"
  }
}
```

**Response (200 OK / 201 Created):**
```json
{
  "migrationId": "d4ob1s530fei623t8sdg",
  "role": "source",
  "ready": true,
  "databasePath": "/path/to/migration.db",
  "rootSummary": {
    "srcRoots": 1,
    "dstRoots": 0
  },
  "sourceConnectionId": "conn-123",
  "destinationConnectionId": ""
}
```

**Notes:**
- `role` must be either `"source"` or `"destination"`
- Returns `201 Created` when `ready: true` (both roots set)
- Returns `200 OK` when only one root is set
- Setting the source root creates a new migration if it doesn't exist

---

## Migration Lifecycle

### List All Migrations

Get a list of all migrations.

**Endpoint:** `GET /migrations`

**Request:** No body required

**Response (200 OK):**
```json
[
  {
    "id": "d4ob1s530fei623t8sdg",
    "name": "My Migration",
    "configPath": "/path/to/config.yaml",
    "createdAt": "2025-01-15T10:30:45Z",
    "isNewMigration": false
  }
]
```

---

### Start Migration

Start a new migration or resume an existing one.

**Endpoint:** `POST /migrations`  
**Legacy:** `POST /migrate/start`

**Request Body:**
```json
{
  "migrationId": "d4ob1s530fei623t8sdg",
  "options": {
    "migrationId": "d4ob1s530fei623t8sdg",
    "databasePath": "/path/to/migration.db",
    "removeExistingDatabase": false,
    "usePreseededDatabase": true,
    "sourceConnectionId": "conn-123",
    "destinationConnectionID": "conn-456",
    "workerCount": 10,
    "maxRetries": 3,
    "coordinatorLead": 4,
    "logAddress": "127.0.0.1:8081",
    "logLevel": "info",
    "enableLoggingTerminal": false,
    "startupDelaySeconds": 0,
    "progressTickMillis": 1000,
    "verification": {
      "allowPending": false,
      "allowFailed": false,
      "allowNotOnSrc": false
    }
  }
}
```

**Response (202 Accepted):**
```json
{
  "id": "d4ob1s530fei623t8sdg",
  "sourceId": "local",
  "destinationId": "spectra-primary",
  "startedAt": "2025-01-15T10:30:45Z",
  "status": "running"
}
```

**Notes:**
- Most options are optional and will use defaults from config
- `databasePath` is optional if roots are already set
- Migration must have both source and destination roots set before starting

---

### Load Migration

Load and resume a migration from its YAML config file.

**Endpoint:** `POST /migrations/{migrationID}/load`

**Request:** No body required

**Response (202 Accepted):**
```json
{
  "id": "d4ob1s530fei623t8sdg",
  "sourceId": "local",
  "destinationId": "spectra-primary",
  "startedAt": "2025-01-15T10:30:45Z",
  "status": "running"
}
```

**Error Responses:**
- `404 Not Found`: Migration not found
- `400 Bad Request`: Migration already running or invalid state

---

### Stop Migration

Stop a running migration and save its state for resumption.

**Endpoint:** `POST /migrations/{migrationID}/stop`

**Request:** No body required

**Response (200 OK):**
```json
{
  "id": "d4ob1s530fei623t8sdg",
  "status": "suspended",
  "result": {
    "rootSummary": {
      "srcRoots": 1,
      "dstRoots": 1
    },
    "runtime": {
      "duration": "2h30m15s",
      "src": {
        "name": "src",
        "round": 5,
        "pending": 0,
        "inProgress": 0,
        "totalTracked": 0,
        "workers": 0
      },
      "dst": {
        "name": "dst",
        "round": 4,
        "pending": 0,
        "inProgress": 0,
        "totalTracked": 0,
        "workers": 0
      }
    },
    "verification": {
      "srcTotal": 1000,
      "dstTotal": 1000,
      "srcPending": 0,
      "dstPending": 0,
      "srcFailed": 0,
      "dstFailed": 0,
      "dstNotOnSrc": 0
    }
  },
  "message": "Migration suspended. State saved for resumption."
}
```

**Error Responses:**
- `404 Not Found`: Migration not found
- `400 Bad Request`: Migration not running

---

## Migration Status & Monitoring

### Get Migration Status

Get the current status of a migration.

**Endpoint:** `GET /migrations/{migrationID}`  
**Legacy:** `GET /migrate/status/{migrationID}`

**Request:** No body required

**Response (200 OK):**
```json
{
  "id": "d4ob1s530fei623t8sdg",
  "sourceId": "local",
  "destinationId": "spectra-primary",
  "startedAt": "2025-01-15T10:30:45Z",
  "status": "running",
  "completedAt": null,
  "error": "",
  "result": {
    "rootSummary": {
      "srcRoots": 1,
      "dstRoots": 1
    },
    "runtime": {
      "duration": "1h23m45s",
      "src": {
        "name": "src",
        "round": 3,
        "pending": 42,
        "inProgress": 8,
        "totalTracked": 50,
        "workers": 4
      },
      "dst": {
        "name": "dst",
        "round": 2,
        "pending": 15,
        "inProgress": 3,
        "totalTracked": 18,
        "workers": 2
      }
    },
    "verification": {
      "srcTotal": 500,
      "dstTotal": 450,
      "srcPending": 50,
      "dstPending": 0,
      "srcFailed": 0,
      "dstFailed": 0,
      "dstNotOnSrc": 0
    }
  }
}
```

**Status Values:**
- `"running"`: Migration is actively processing
- `"completed"`: Migration finished successfully
- `"suspended"`: Migration was stopped and can be resumed
- `"failed"`: Migration encountered an error

---

### Inspect Migration Status

Get detailed migration status from the database (includes internal state).

**Endpoint:** `GET /migrations/{migrationID}/inspect`

**Request:** No body required

**Response (200 OK):**
```json
{
  "isEmpty": false,
  "hasPending": true,
  "hasFailures": false,
  "srcTotal": 500,
  "dstTotal": 450,
  "srcPending": 50,
  "dstPending": 0,
  "srcFailed": 0,
  "dstFailed": 0
}
```

**Error Responses:**
- `404 Not Found`: Migration not found or database doesn't exist

---

### Get Queue Metrics

Get real-time queue statistics for a running migration.

**Endpoint:** `GET /migrations/{migrationID}/queue-metrics`

**Request:** No body required

**Response (200 OK):**
```json
{
  "srcTraversal": {
    "name": "src",
    "round": 3,
    "pending": 42,
    "inProgress": 8,
    "totalTracked": 50,
    "workers": 4,
    "averageExecutionTime": 125000000,
    "tasksPerSecond": 12.5,
    "totalCompleted": 1250,
    "lastPollTime": "2025-01-15T10:30:45.123456Z"
  },
  "dstTraversal": {
    "name": "dst",
    "round": 2,
    "pending": 15,
    "inProgress": 3,
    "totalTracked": 18,
    "workers": 2,
    "averageExecutionTime": 98000000,
    "tasksPerSecond": 8.2,
    "totalCompleted": 450,
    "lastPollTime": "2025-01-15T10:30:45.123456Z"
  },
  "copy": null
}
```

**Notes:**
- Queue objects may be `null` if the queue hasn't started yet
- `averageExecutionTime` is in nanoseconds (divide by 1e6 for milliseconds)
- Poll every 200ms - 1s for real-time updates
- Observer updates metrics every 200ms

**Error Responses:**
- `404 Not Found`: Migration not found
- `500 Internal Server Error`: Database access error

---

### Stream Migration Progress

Stream real-time migration progress events via Server-Sent Events (SSE).

**Endpoint:** `GET /migrations/{migrationID}/stream`  
**Legacy:** `GET /migrate/status/{migrationID}/stream`

**Request:** No body required

**Response:** Server-Sent Events stream

**Event Format:**
```
event: progress
data: {"event":"progress","timestamp":"2025-01-15T10:30:45Z","migration":{"id":"d4ob1s530fei623t8sdg","status":"running"},"source":{"round":3,"pending":42,"inProgress":8,"totalTracked":50,"workers":4},"destination":{"round":2,"pending":15,"inProgress":3,"totalTracked":18,"workers":2}}

event: completed
data: {"event":"completed","timestamp":"2025-01-15T10:30:45Z","migration":{"id":"d4ob1s530fei623t8sdg","status":"completed"},"source":{},"destination":{}}
```

**Event Types:**
- `progress`: Periodic progress updates with queue stats
- `started`: Migration started
- `completed`: Migration completed
- `suspended`: Migration was stopped
- `failed`: Migration failed

**Usage:**
```javascript
const eventSource = new EventSource(`/migrations/${migrationID}/stream`);
eventSource.addEventListener('progress', (e) => {
  const data = JSON.parse(e.data);
  // Update UI with progress
});
```

---

## Database Management

### Upload Migration Database

Upload a migration database file to the server.

**Endpoint:** `POST /migrations/db/upload`

**Request:** Multipart form data
- `file` (file, required): The database file (.db)
- `filename` (string, required): Filename for the database
- `overwrite` (string, optional): Set to `"true"` to overwrite existing file

**Response (200 OK):**
```json
{
  "success": true,
  "path": "/path/to/uploaded/file.db"
}
```

**Error Responses:**
- `400 Bad Request`: Missing file or filename, or file already exists (unless overwrite=true)
- `500 Internal Server Error`: Upload failed

**Example (curl):**
```bash
curl -X POST \
  -F "file=@migration.db" \
  -F "filename=migration.db" \
  -F "overwrite=false" \
  http://localhost:8080/api/migrations/db/upload
```

---

### List Migration Databases

List all available migration database files.

**Endpoint:** `GET /migrations/db/list`

**Request:** No body required

**Response (200 OK):**
```json
[
  {
    "filename": "d4ob1s530fei623t8sdg.db",
    "path": "/path/to/d4ob1s530fei623t8sdg.db",
    "size": 1048576,
    "modifiedAt": "2025-01-15T10:30:45Z"
  }
]
```

---

## Logging

### Get Migration Logs

Retrieve the latest logs from a migration database.

**Endpoint:** `POST /migrations/{migrationID}/logs`

**Request Body:**
```json
{}
```

**Note:** Request body is optional (can be empty object). Always returns up to 1,000 logs per level.

**Response (200 OK):**
```json
{
  "logs": {
    "trace": [
      {
        "id": "log-id-999",
        "level": "trace",
        "data": {
          "message": "Task started",
          "timestamp": "2025-01-15T10:30:45.123456Z",
          "taskId": "task-123"
        }
      }
    ],
    "debug": [
      {
        "id": "log-id-998",
        "level": "debug",
        "data": {
          "message": "Processing file",
          "path": "/path/to/file.txt"
        }
      }
    ],
    "info": [],
    "warning": [],
    "error": [],
    "critical": []
  }
}
```

**Notes:**
- Returns up to 1,000 logs per level (newest first)
- Logs are ordered descending by ID (newest first)
- Missing buckets return empty arrays
- Log levels: `trace`, `debug`, `info`, `warning`, `error`, `critical`
- UI should filter out duplicate logs on the client side when polling

**Error Responses:**
- `404 Not Found`: Migration not found
- `400 Bad Request`: Invalid request body
- `500 Internal Server Error`: Database access error

**Usage Example:**
```typescript
// Poll logs periodically
const response = await fetch(`/migrations/${migrationID}/logs`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({})
});

const { logs } = await response.json();

// Filter out duplicates on client side using log IDs
// Logs are returned newest first, so you can track the newest ID you've seen
```

---

### Toggle Log Terminal

Enable or disable the logging terminal for live log display.

**Endpoint:** `POST /migrations/log-terminal`

**Request Body:**
```json
{
  "enable": true,
  "logAddress": "127.0.0.1:8081"
}
```

**Response (200 OK):**
```json
{
  "enabled": true,
  "message": "log terminal toggled successfully"
}
```

**Notes:**
- `logAddress` is optional if already configured
- Used for local development to display live UDP logs in a terminal

---

## Error Responses

All endpoints may return the following error responses:

### 400 Bad Request
```json
{
  "error": "migration id is required"
}
```

### 404 Not Found
```json
{
  "error": "migration not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "failed to start migration"
}
```

---

## Common Patterns

### Migration Workflow

1. **Set Source Root**
   ```javascript
   POST /migrations/roots
   { "role": "source", "serviceId": "local", "root": {...} }
   ```

2. **Set Destination Root**
   ```javascript
   POST /migrations/roots
   { "role": "destination", "serviceId": "spectra-primary", "root": {...} }
   ```

3. **Start Migration**
   ```javascript
   POST /migrations
   { "migrationId": "...", "options": {...} }
   ```

4. **Monitor Progress**
   ```javascript
   // Option 1: Poll status
   GET /migrations/{migrationID}
   
   // Option 2: Stream events
   GET /migrations/{migrationID}/stream
   
   // Option 3: Poll queue metrics
   GET /migrations/{migrationID}/queue-metrics
   ```

5. **Get Logs**
   ```javascript
   POST /migrations/{migrationID}/logs
   {}
   ```

6. **Stop Migration (if needed)**
   ```javascript
   POST /migrations/{migrationID}/stop
   ```

### Polling Recommendations

- **Status**: Poll every 1-2 seconds
- **Queue Metrics**: Poll every 200ms - 1s (observer updates every 200ms)
- **Logs**: Poll every 500ms - 1s (filter duplicates on client side using log IDs)

---

## Type Definitions

### FolderDescriptor
```typescript
{
  id: string;
  parentId?: string;
  parentPath?: string;
  displayName?: string;
  locationPath?: string;
  lastUpdated?: string;
  depthLevel?: number;
  type?: string;
}
```

### MigrationOptions
```typescript
{
  migrationId?: string;
  databasePath?: string;
  removeExistingDatabase?: boolean;
  usePreseededDatabase?: boolean;
  sourceConnectionId?: string;
  destinationConnectionId?: string;
  workerCount?: number;
  maxRetries?: number;
  coordinatorLead?: number;
  logAddress?: string;
  logLevel?: string;
  enableLoggingTerminal?: boolean;
  startupDelaySeconds?: number;
  progressTickMillis?: number;
  verification?: {
    allowPending?: boolean;
    allowFailed?: boolean;
    allowNotOnSrc?: boolean;
  };
}
```

---

## Notes

- All timestamps are in ISO 8601 format (UTC)
- All endpoints require authentication (JWT token in Authorization header)
- Base URL is typically `/api` in production
- Legacy endpoints are maintained for backward compatibility
- Database operations are read-only for monitoring endpoints
- Missing buckets/queues return empty/null values gracefully

