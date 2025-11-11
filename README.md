# Sylos API

Middleware/API layer for Sylos that wraps the Core Migration Engine and exposes REST endpoints for the Wails UI (and future clients).

## Features

- Lightweight Go HTTP server (Chi) with structured logging (zerolog)
- JWT-auth scaffolding for local UI ↔ API communication
- Placeholder adapters to the Core Migration Engine
- Makefile helpers for building, testing, and running locally

## Getting Started

1. Install Go 1.22 or newer.
2. Clone the repo and initialize environment variables (or copy `config.example.yaml` to `config.yaml`).
3. Set `SYLOS_JWT_SECRET` (or update the config) so the API can sign auth tokens.
4. Run the server:

```bash
make run
```

The server listens on `localhost:8080` by default.

## Configuration

Configuration is loaded via [`pkg/config`](pkg/config):

- Environment variables prefixed with `SYLOS_` (e.g., `SYLOS_HTTP_PORT`)
- Optional `config.yaml` file in the project root (see `config.example.yaml`)
- `SYLOS_CONFIG_PATH` can point to an alternate config file

Key settings:

- `http.port`: Port for the HTTP listener.
- `jwt.secret`: Symmetric signing key for JWTs.
- `jwt.access_token_ttl`: Access token lifetime (e.g., `15m`, `1h`).

## API Endpoints

- `POST /api/auth/login` — exchange credentials for a JWT (currently stubbed).
- `GET /api/health` — authenticated health probe.
- `GET /api/source/list` — list available migration sources.
- `POST /api/migrate/start` — initiate a migration.
- `GET /api/migrate/status/{migrationID}` — fetch migration status.
- `GET /health` — unauthenticated server health check.

Routes under `/api` require a Bearer token in the `Authorization` header.

## Project Layout

```
cmd/server        // entrypoint and wiring
internal/api      // HTTP handlers and routing
internal/auth     // JWT helpers and middleware
internal/corebridge // adapters/hooks into the Core library (stubbed for now)
internal/server   // HTTP server wrapper
pkg/config        // config loader
pkg/logger        // zerolog setup
```

## Next Steps

- Integrate real authentication/identity provider.
- Replace `internal/corebridge` mock with calls into `github.com/Project-Sylos/Migration-Engine`.
- Add persistence for migration state and user ↔ migration mappings.
- Expand API surface as the Core library features mature.

