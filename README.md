# Sylos API

Public REST layer for Sylos that sits between the Wails desktop UI and the Migration Engine SDK. It authenticates UI requests, exposes filesystem browsing helpers, and orchestrates migrations by delegating directly to `github.com/Project-Sylos/Migration-Engine`.

---

## Highlights

- **Thin but explicit router**: Chi-based route tree with domain packages (`auth`, `services`, `migrations`, `health`) so each concern lives in a focused module.
- **Core bridge manager**: `internal/corebridge` translates REST requests into Migration Engine SDK calls, handles adapter lifecycle (local filesystem, Spectra), and tracks run state.
- **Config-driven services**: `config.yaml` (or `SYLOS_*` env vars) define available connectors and queue/log tuning; the API validates and normalizes everything at startup.
- **Structured logging + graceful shutdown**: Zerolog for consistent logging, plus proper signal handling so queue workers have time to drain.
- **Local-first ergonomics**: Go toolchain 1.24.2, `make run/test/tidy`, and local `replace` directives for developing alongside the Migration Engine and Spectra repos.

---

## Quick Start

1. Install Go `1.24.2` (matching the `go.mod` and toolchain directive).
2. Place the sibling repositories next to this one:
   ```
   /home/you/GitHub/
     ├─ Migration-Engine/
     ├─ Spectra/
     └─ Sylos-API/
   ```
   The `replace` directives in `go.mod` point to `../Migration-Engine` and `../Spectra` for local development.
3. Copy the sample config and set a JWT secret:
   ```bash
   cp config.example.yaml config.yaml
   export SYLOS_JWT_SECRET="super-secret-key"
   ```
4. Adjust `config.yaml` (see [Configuration](#configuration)) to declare the local/Spectra services you want to expose.
5. Boot the server:
   ```bash
   make run
   ```
   By default it listens on `http://localhost:8080`.
6. Run the usual sanity checks:
   ```bash
   make test
   ```

---

## Architecture Overview

```
Wails UI ──▶ Sylos API (this repo) ──▶ Migration Engine SDK ──▶ queue/fsservices/db/logservice
                 │
                 ├─ Auth routes (JWT issuance, currently stubbed)
                 ├─ Service routes (list connectors, browse folders)
                 ├─ Migration routes (launch runs, fetch status/results)
                 └─ Health routes (public + authenticated)
```

- `cmd/server` wires configuration, logging, the core bridge manager, and the Chi router, then starts an HTTP server with graceful shutdown semantics.
- `pkg/config` loads YAML + environment overrides, normalizes absolute data directories, and builds the allow-listed service catalog (local filesystem roots, Spectra worlds, etc.).
- `internal/corebridge` is the translation layer:
  - normalizes requested folder descriptors into `fsservices.Folder`,
  - instantiates the appropriate adapter (local or Spectra) for each run,
  - seeds the migration, triggers `migration.LetsMigrate`, and records results,
  - keeps in-memory state for active/completed migrations.
- `internal/routes` contains one package per route group (`auth`, `services`, `migrations`, `health`) plus the top-level `routes` package that composes them and applies middleware.
- `internal/auth` holds JWT helpers (signing, middleware). Authentication is intentionally minimal today; use environment variables or config to tighten it before exposing to untrusted clients.
- `pkg/logger` and `internal/server` provide structured logging and `http.Server` wrappers.

---

## Configuration

Configuration flows through [`pkg/config`](pkg/config). Sources:

- Environment variables (`SYLOS_*`, e.g. `SYLOS_HTTP_PORT=9090`).
- `config.yaml` in the project root (copy from `config.example.yaml`).
- Optional `SYLOS_CONFIG_PATH` pointing to a different file.

Key sections (see `config.example.yaml` for full references):

```yaml
environment: development
http:
  port: 8080
jwt:
  secret: "change-me"            # required
  access_token_ttl: "15m"
runtime:
  data_dir: "./data"             # outputs DBs, logs here (normalized to absolute path)
  log_address: "127.0.0.1:8081"  # optional UDP listener for live logs
  log_level: "info"
  skip_log_listener: true
  default_worker_count: 10
  default_max_retries: 3
  default_coordinator_lead: 4
services:
  local:
    - id: "local-default"
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

- **JWT secret** must be supplied either in config or via `SYLOS_JWT_SECRET`; the API will refuse to start otherwise.
- **Local services**: users can only browse within the configured `root_path` (the service enforces prefix checks).
- **Spectra services**: each entry identifies a config file and world; the API spawns a temporary Spectra SDK client per request.
- **Runtime data**: migration databases and log buffers are written to `${runtime.data_dir}/${migrationID}.db`. The directory is created automatically.
- **UDP logging**: set `runtime.log_address` and leave `skip_log_listener=false` to spawn a listener for live logs (matching the Migration Engine SDK behavior).

---

## Routes & Middleware

### Public routes

| Method | Path        | Purpose                    |
|--------|-------------|----------------------------|
| GET    | `/health`   | Basic health probe         |
| POST   | `/api/auth/login` | Issue a JWT (stub logic today) |

### Authenticated routes (`Authorization: Bearer <token>`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Authenticated health check |
| GET | `/api/services` | List configured service connectors (local + Spectra) |
| GET | `/api/source/list` | Legacy alias for `GET /api/services` |
| GET | `/api/services/{serviceID}/children?identifier=` | List folders/files under a given service node |
| POST | `/api/migrations` | Start a migration; body matches `corebridge.StartMigrationRequest` |
| POST | `/api/migrate/start` | Legacy alias |
| GET | `/api/migrations/{migrationID}` | Fetch migration status/result summary |
| GET | `/api/migrate/status/{migrationID}` | Legacy alias |
| GET | `/api/migrations/{migrationID}/stream` | Server-Sent Events (SSE) stream of progress events |
| GET | `/api/migrate/status/{migrationID}/stream` | Legacy alias for the SSE stream |

### Route packages

- `internal/routes/auth`: defines `Register` and the login handler; includes JSON helpers.
- `internal/routes/services`: registers list/browse routes and enforces service validation.
- `internal/routes/migrations`: starts migrations, streams status, and returns structured results (queue stats, verification report).
- `internal/routes/health`: exposes both public and authenticated health endpoints.
- `internal/routes/routes.go`: constructs the main router, applies common middleware (request ID, real IP, recovery, logging), mounts `/api`, and attaches the JWT middleware.

---

## Response Models

Select responses are backed by the types exported from `internal/corebridge`:

- `Service`: `id`, `displayName`, `type` (`local` or `spectra`), plus connector metadata.
- `ListChildren`: returns `fsservices.ListResult` (arrays of folders/files with consistent metadata).
- `Migration`: `id`, `sourceId`, `destinationId`, `startedAt`, `status`.
- `Status`: extends `Migration` with `completedAt`, `error`, and `result`.
- `Result`: includes `rootSummary`, `runtime` queue stats, and `verification` report from the Migration Engine SDK.
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
- Enable UDP listener for local development via `runtime.log_address` and `runtime.skip_log_listener=false`; logs also persist in the migration database via the SDK.
- Progress streams are delivered over SSE. Heartbeats (`: heartbeat`) are sent every 30 seconds to keep connections alive; reconnect on network drops to continue receiving updates.

---

## Building & Testing

Common targets (see [`Makefile`](Makefile)):

```bash
make build   # go build ./...
make run     # go run ./cmd/server
make test    # go test ./...
make tidy    # go mod tidy
make fmt     # go fmt ./...
```

The codebase assumes Go `1.24.2`. If you use `gorr` or other tools, ensure they respect the toolchain directive in `go.mod`.

---

## Security Considerations

- JWT issuance is currently stubbed (accepts any username/password). Before exposing the API publicly, integrate a real identity provider or enforce strong local auth.
- Local filesystem connectors should point to dedicated allow-listed roots; the service prevents escaping those roots but you are responsible for which directories you expose.
- HTTPS termination is out-of-scope for this repo—run behind a reverse proxy (nginx, Caddy, etc.) or load balancer that enforces TLS.
- Long-running migrations store their SQLite databases under `runtime.data_dir`; ensure appropriate filesystem permissions and cleanup policies for production environments.

---

## Roadmap

- Harden authentication/authorization (multi-user policies, token refresh, etc.).
- Add connectors for additional cloud providers (S3, Azure, Google Drive, …) once the Migration Engine supports their adapters.
- Surface live progress streams (SSE/WebSocket) rather than polling status endpoints.
- Persist historical migration metadata to an external store for auditing.
- Provide CLI tooling that reuses the same API for scripting workflows.

---

## License

This project is released under the same license as the broader Sylos ecosystem. Refer to `LICENSE` for details.

