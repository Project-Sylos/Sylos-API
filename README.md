# Sylos API

Public REST layer for Sylos that sits between the browser UI (Sylos-UI / embedded Sylos binary) and the Migration Engine SDK. It authenticates UI requests, exposes filesystem browsing helpers, and orchestrates migrations by delegating to `codeberg.org/Sylos/Migration-Engine` and Sylos-FS adapters.

**Migration Engine boundary:** almost all engine access goes through `pkg/migration` (`internal/corebridge`). Exceptions: `apidb` opens the DB via `pkg/db` (blank-imports `pkg/db/seal`), path-check target resolution uses `pkg/queue/gpl`, and metrics DTO mapping uses `pkg/convert`. Queue observe/worker/mode and scaling loop/profile internals are not API dependencies.

---

## Highlights

- **Thin but explicit router**: Chi-based route tree with domain packages (`auth`, `users`, `services`, `providers`, `migrations`, `health`, …) so each concern lives in a focused module.
- **Core bridge manager**: `internal/corebridge` translates REST requests into Migration Engine calls, handles adapter lifecycle (local, Spectra, cloud), and tracks run state.
- **Baked-in defaults**: `pkg/config` hardcodes production defaults (port, OS app-data dirs, provider catalog, local FS). No YAML config file.
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
3. Boot the server (no config file):
   ```bash
   make run
   ```
   By default it listens on `http://localhost:8086` and stores data under the OS app-data directory. A working OS keyring is required.
4. Run the usual sanity checks:
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
- `pkg/config` returns baked-in defaults, creates OS app-data and migration-dbs directories, and builds the service/provider catalog.
- `internal/corebridge` is the translation layer:
  - **`Bridge`** interface and request/response types (`bridge.go`, `types_*.go`),
  - **`manager`** implements the bridge (roots, lifecycle, progress, uploads, providers),
  - **`migrationops`** holds Migration Engine / path-review helpers used by manager and routes,
  - **`migrationfiles`** resolves migration DB paths and upload/list/clean helpers,
  - instantiates Sylos-FS adapters, seeds roots, and drives domain phase methods (`StartTraversal`, copy, delete, retry),
  - keeps in-memory state for live migrations and background tasks.
- `internal/routes` contains one package per route group (`auth`, `users`, `services`, `providers`, `migrations`, `health`, …) plus the top-level `routes` package that composes them and applies middleware.
- `internal/auth` holds JWT helpers, roles, and the users store (Badger-backed via `sylos.api/`).
- `pkg/logger` and `internal/server` provide structured logging and `http.Server` wrappers.

---

## Configuration

There is no `config.yaml`. [`pkg/config.Load`](pkg/config) returns baked-in production defaults:

- HTTP port `8086` (Sylos launcher can override with `--port`)
- Data under the OS app-data directory (`org.sylos.app`), with `migration-dbs/` underneath
- Full cloud provider catalog enabled; local filesystem with empty root (unrestricted browse)
- No Spectra connectors in the shipped catalog
- Install master key from the OS keyring only (see [`docs/ENCRYPTION.md`](docs/ENCRYPTION.md))

JWT secret is generated on first startup and stored in `sylos.api/` install config. Non-Google OAuth apps may be seeded from build-time ldflags (`internal/oauthbuiltin`), filled from the Sylos launcher’s `oauth.env` + `oauth.key` (see the Sylos README).

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
- Long-running migrations store DB files under `runtime.data_dir`; see [`docs/ENCRYPTION.md`](docs/ENCRYPTION.md) for key handling and backup policy.

---

## Roadmap

- Harden authentication/authorization (multi-user policies, token refresh, etc.).
- Expand cloud provider coverage as Sylos-FS adapters land.
- Persist richer historical migration metadata for auditing.
- Provide CLI tooling that reuses the same API for scripting workflows.

---

## License

This project is released under the same license as the broader Sylos ecosystem. Refer to `LICENSE` for details.

