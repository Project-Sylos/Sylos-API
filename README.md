# Sylos API

Public REST layer for Sylos that sits between the browser UI (Sylos-UI / embedded Sylos binary) and the Migration Engine SDK. It authenticates UI requests, exposes filesystem browsing helpers, and orchestrates migrations by delegating to `codeberg.org/Sylos/Migration-Engine` and Sylos-FS adapters.

**Migration Engine boundary:** almost all engine access goes through `pkg/migration` (`internal/corebridge`). Exceptions: `apidb` opens DuckDB via `pkg/db` (blank-imports `pkg/db/seal`), path-check target resolution uses `pkg/queue/gpl`, and metrics DTO mapping uses `pkg/convert`. Queue observe/worker/mode and scaling loop/profile internals are not API dependencies.

---

## Highlights

- **Thin but explicit router**: Chi-based route tree with domain packages (`auth`, `users`, `services`, `providers`, `migrations`, `health`, …) so each concern lives in a focused module.
- **Core bridge manager**: `internal/corebridge` translates REST requests into Migration Engine calls, handles adapter lifecycle (local, Spectra, cloud), and tracks run state.
- **Config-driven services**: `config.yaml` (or `SYLOS_*` env vars) define available connectors and queue/log tuning; the API validates and normalizes everything at startup.
- **Structured logging + graceful shutdown**: Zerolog for consistent logging, plus proper signal handling so queue workers have time to drain.
- **Local-first ergonomics**: Go toolchain matching `go.mod`, `make run/test/tidy`, and local `replace` directives for developing alongside Migration Engine, Sylos-FS, and Spectra.

---

## Quick Start

1. Install Go matching `go.mod` (currently 1.26.x).
2. Place the sibling repositories next to this one:
   ```
   /home/you/GitHub/
     ├─ Migration-Engine/
     ├─ Sylos-FS/
     ├─ Spectra/
     └─ Sylos-API/
   ```
   The `replace` directives in `go.mod` point at those siblings for local development.
3. Create `config.yaml` (defaults work for local use) and optionally set a JWT secret:
   ```bash
   export SYLOS_JWT_SECRET="super-secret-key"
   ```
4. Adjust `config.yaml` (see [Configuration](#configuration)) to declare the local/Spectra services you want to expose.
5. Boot the server:
   ```bash
   make run
   ```
   By default it listens on `http://localhost:8086`.
6. Run the usual sanity checks:
   ```bash
   make test
   ```

   For the full product (embedded UI + this API), build and run the sibling **Sylos** launcher instead.

---

## Architecture Overview

```
Sylos-UI / browser ──▶ Sylos API (this repo) ──▶ Migration Engine ──▶ queue / db / scaling / logservice
                              │                         │
                              │                         └─▶ Sylos-FS adapters
                              ├─ Auth / users (JWT, roles)
                              ├─ Services / providers / OAuth apps
                              ├─ Migration routes (lifecycle, review, status/SSE)
                              └─ Health routes
```

- `main.go` / `pkg/app` wires configuration, logging, the core bridge manager, and the Chi router, then starts an HTTP server with graceful shutdown semantics.
- `pkg/config` loads YAML + environment overrides, normalizes absolute data directories, and builds the allow-listed service catalog (local filesystem roots, Spectra worlds, etc.).
- `internal/corebridge` is the translation layer:
  - **`Bridge`** interface and request/response types (`bridge.go`, `types_*.go`),
  - **`manager`** implements the bridge (roots, lifecycle, progress, uploads, providers),
  - **`migrationops`** holds Migration Engine / path-review helpers used by manager and routes,
  - **`migrationfiles`** resolves migration DB paths and upload/list/clean helpers,
  - instantiates Sylos-FS adapters, seeds roots, and drives domain phase methods (`StartTraversal`, copy, delete, retry),
  - keeps in-memory state for live migrations and background tasks.
- `internal/routes` contains one package per route group (`auth`, `users`, `services`, `providers`, `migrations`, `health`, …) plus the top-level `routes` package that composes them and applies middleware.
- `internal/auth` holds JWT helpers, roles, and the users store (DuckDB-backed via `sylos.duckdb`).
- `pkg/logger` and `internal/server` provide structured logging and `http.Server` wrappers.

---

## Configuration

Configuration flows through [`pkg/config`](pkg/config). Sources:

- Environment variables (`SYLOS_*`, e.g. `SYLOS_HTTP_PORT=9090`).
- `config.yaml` in the project root (create from defaults or copy from a known-good install).
- Optional `SYLOS_CONFIG_PATH` pointing to a different file.

Key sections:

```yaml
environment: development
http:
  port: 8086
jwt:
  secret: "change-me"            # optional; random secret generated if omitted
  access_token_ttl: "15m"
runtime:
  data_dir: "./data"             # outputs DBs, logs here (normalized to absolute path)
  log_address: "127.0.0.1:8081"  # optional UDP listener for live logs
  log_level: "info"
  enable_logging_terminal: false
  default_worker_count: 10
  default_max_retries: 3
  default_coordinator_lead: 4
services:
  local:
    - id: "local"
      name: "Local Filesystem"
      root_path: "/path/to/allowlisted/root"   # server-side absolute path
  spectra:
    - id: "spectra-primary"
      name: "Spectra Primary"
      config_path: "../Migration-Engine/pkg/configs/spectra.json"
      world: "primary"
      root_id: "root"
```

Important notes:

- **JWT secret**: if omitted, the server generates a random secret on first startup and stores it in the encrypted `sylos.duckdb` `install_config` table. Set `SYLOS_JWT_SECRET` or `jwt.secret` in config to override.
- **Local services**: users can only browse within the configured `root_path` (the service enforces prefix checks).
- **Spectra services**: each entry identifies a config file and world; the API spawns a temporary Spectra SDK client per request.
- **Runtime data**: migration databases and log buffers are written under `${runtime.data_dir}/${migrationID}/`. The directory is created automatically.
- **Logging terminal**: set `runtime.enable_logging_terminal=true` and `runtime.log_address` to automatically spawn a log terminal process when starting migrations. The terminal displays live UDP logs from the migration engine. You can also toggle it mid-run via `POST /api/migrations/log-terminal`.
- **Encryption**: see [`docs/ENCRYPTION.md`](docs/ENCRYPTION.md) for install master key and per-migration DuckDB encryption.

---

## Routes & Middleware

### Public routes

| Method | Path        | Purpose                    |
|--------|-------------|----------------------------|
| GET    | `/health`   | Basic health probe         |
| POST   | `/api/auth/login` | Issue a JWT (username/password against the users store) |

### Authenticated routes (`Authorization: Bearer <token>`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Authenticated health check |
| GET | `/api/services` | List configured service connectors (local + Spectra) |
| GET | `/api/source/list` | Legacy alias for `GET /api/services` |
| GET | `/api/services/{serviceID}/children?identifier=` | List folders/files under a given service node |
| POST | `/api/migrations/roots` | Seed database with selected source/destination roots and receive a migration id |
| POST | `/api/migrations` | Start a migration; body matches `corebridge.StartMigrationRequest` |
| POST | `/api/migrate/start` | Legacy alias |
| POST | `/api/migrations/log-terminal` | Toggle log terminal on/off mid-run; body: `{"enable": true, "logAddress": "127.0.0.1:8081"}` (logAddress optional) |
| GET | `/api/migrations/{migrationID}` | Fetch migration status/result summary |
| GET | `/api/migrate/status/{migrationID}` | Legacy alias |
| GET | `/api/migrations/{migrationID}/stream` | Server-Sent Events (SSE) stream of progress events |
| GET | `/api/migrate/status/{migrationID}/stream` | Legacy alias for the SSE stream |

Typical flow for the UI:

1. `GET /api/services` → populate source/destination pickers.
2. `GET /api/services/{serviceID}/children` → browse folders until the user selects a root.
3. Call `POST /api/migrations/roots` **once per role** (`role: "source"` or `"destination"`) with the selected `serviceId`, optional `connectionId`, and folder descriptor. The first call stores the root; after the second role is set the API seeds the database and responds with `ready: true`, the shared `migrationId`, and any connection ids to reuse. Example payload:
   ```json
   {
     "migrationId": "abc123",
     "role": "source",
     "serviceId": "spectra-primary",
     "connectionId": "shared-spectra-session",
     "root": {
       "id": "root",
       "displayName": "Root",
       "locationPath": "/",
       "type": "folder"
     }
   }
   ```
4. `POST /api/migrations` with the `migrationId` (either top-level `migrationId` field or inside `options.migrationId`) → trigger traversal/execution. Reuse the same connection ids so shared connectors stay in sync. Minimal payload:
   ```json
   { "migrationId": "abc123" }
   ```
5. `GET /api/migrations/{migrationID}/stream` → stream progress until `event: completed` or `event: failed` arrives.

### Route packages

- `internal/routes/auth`: login and JWT issuance.
- `internal/routes/users`: user admin CRUD (role-gated).
- `internal/routes/services`: list/browse routes and service validation.
- `internal/routes/providers` / `oauthapps`: cloud provider and OAuth app configuration.
- `internal/routes/migrations`: migration lifecycle, review, status, SSE; see [`internal/routes/migrations/README.md`](internal/routes/migrations/README.md).
- `internal/routes/health`: public and authenticated health endpoints.
- `internal/routes/routes.go`: constructs the main router, applies common middleware, mounts `/api`, and attaches the JWT middleware.

---

## Response Models

Select responses are backed by the types exported from `internal/corebridge` (`bridge.go`, `types_*.go`):

- `Source` / service listing: connector id, display name, type, plus connector metadata.
- `ListChildrenResponse`: folders/files with consistent browse metadata.
- `Migration`: `id`, `sourceId`, `destinationId`, `startedAt`, `status`.
- `Status`: extends `Migration` with `completedAt`, `error`, and `result`.
- `Result`: includes `rootSummary`, `runtime` queue stats, and `verification` report from the Migration Engine.
- `SetRootResponse`: `{ migrationId, role, ready, databasePath?, rootSummary?, sourceConnectionId?, destinationConnectionId? }` — when `ready` is true both roots are set and the database has been seeded for `/api/migrations`.
- `ProgressEvent` (SSE stream):
  ```json
  {
    "event": "running",
    "timestamp": "2025-01-03T10:15:30Z",
    "migration": { ...Status object... },
    "source": { "round": 1, "pending": 5, "inProgress": 2, "totalTracked": 12, "workers": 10 },
    "destination": { "round": 0, "pending": 3, "inProgress": 1, "totalTracked": 6, "workers": 10 }
  }
  ```
  Events you may see: `snapshot` (initial state), `started`, `running` (heartbeat), `completed`, `failed`, and `close`.

All error responses follow `{ "error": "message" }`.

---

## Logging & Monitoring

- Every HTTP request is logged via Chi middleware (method, path, status, bytes, duration).
- The core bridge logs migration lifecycle, including successes/failures, and can forward logs to the Migration Engine’s UDP listener if configured.
- Enable logging terminal for local development via `runtime.enable_logging_terminal=true` and `runtime.log_address`; this spawns a terminal process that displays live UDP logs. You can also toggle it mid-run via `POST /api/migrations/log-terminal`. Logs also persist in the migration database via the SDK.
- Progress streams are delivered over SSE. Heartbeats (`: heartbeat`) are sent every 30 seconds to keep connections alive; reconnect on network drops to continue receiving updates.

---

## Building & Testing

Common targets (see [`Makefile`](Makefile)):

```bash
make build   # go build ./...
make run     # go run .
make test    # go test ./...
make tidy    # go mod tidy
make fmt     # go fmt ./...
```

The codebase assumes the Go version in `go.mod`. If you use `gorr` or other tools, ensure they respect the toolchain directive.

---

## Security Considerations

- Authentication uses the users store (roles, first-run admin). Before exposing the API publicly, enforce strong passwords and network controls.
- Local filesystem connectors should point to dedicated allow-listed roots; the service prevents escaping those roots but you are responsible for which directories you expose.
- HTTPS termination is out-of-scope for this repo—run behind a reverse proxy (nginx, Caddy, etc.) or load balancer that enforces TLS.
- Long-running migrations store DuckDB files under `runtime.data_dir`; see [`docs/ENCRYPTION.md`](docs/ENCRYPTION.md) for key handling and backup policy.

---

## Roadmap

- Harden authentication/authorization (multi-user policies, token refresh, etc.).
- Expand cloud provider coverage as Sylos-FS adapters land.
- Persist richer historical migration metadata for auditing.
- Provide CLI tooling that reuses the same API for scripting workflows.

---

## License

This project is released under the same license as the broader Sylos ecosystem. Refer to `LICENSE` for details.

