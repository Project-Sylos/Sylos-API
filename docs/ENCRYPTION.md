# DuckDB envelope encryption

Sylos uses a two-tier encryption model for data at rest.

## Keys

| Key | Storage | Protects |
|-----|---------|----------|
| **Install master key** (32 bytes) | OS keyring (`Sylos` / `install-master-key`) by default, or `creds/.env` with `--use-env-keys` | `sylos.duckdb` (users, migration registry, per-migration keys, provider OAuth app credentials) |
| **Per-migration key** (32 bytes) | Plaintext row in `migration_keys` inside encrypted `sylos.duckdb` | Each `{dataDir}/{id}/{id}.db` migration database |

Default mode requires a working OS keyring; startup fails if the key cannot be read or stored there.

With `--use-env-keys`, `SYLOS_MASTER_KEY` overrides `creds/.env`. Setting `SYLOS_MASTER_KEY` or `SYLOS_KEY_FILE` without `--use-env-keys` is rejected at startup.

There is no plaintext fallback file for the install master key in default mode.

## What is encrypted

- API database file (`sylos.duckdb`), including WAL and temp files (DuckDB 1.4+ native encryption)
- Per-migration DuckDB files when opened through the API
- OAuth refresh tokens are stored as plaintext JSON rows inside encrypted migration DBs (not field-level AES)

## What is not encrypted

- DuckDB main header metadata (version/canary) — documented DuckDB limitation
- JWT signing secret (`install_config` row in encrypted `sylos.duckdb`, or `SYLOS_JWT_SECRET` / `jwt.secret` in config)
- In-memory OAuth access tokens

## Backup and recovery

**Losing the install master key means total loss of `sylos.duckdb` and all per-migration keys.** Back up one of:

- OS keyring export for service `Sylos` / user `install-master-key`
- `creds/.env` when using `--use-env-keys`
- `creds/.env` or `SYLOS_MASTER_KEY` when using `--use-env-keys`

Per-migration `.db` files are useless without both the file and the corresponding key from `migration_keys`.

## Operator flags

```bash
./bin/sylos                  # default: keyring
./bin/sylos --use-env-keys   # read/write SYLOS_MASTER_KEY in creds/.env
```

## Validation checklist

Before relying on encryption in production:

- [ ] Bundled DuckDB ≥ 1.4.0 with encryption enabled
- [ ] WAL and temp files encrypted (create migration, checkpoint, inspect `.wal`)
- [ ] Stolen `sylos.duckdb` without master key is unreadable
- [ ] Stolen migration `.db` without per-migration key is unreadable
- [ ] Wrong master key fails fast at startup with a clear error
- [ ] `pkg/tests` scenarios still use plaintext DBs (`EncryptionKey == nil`)

## Threat model notes

- DuckDB encryption does not yet meet NIST requirements (per DuckDB docs).
- Provider OAuth app credentials (`client_id` / `client_secret`) live in the encrypted API DB; configure in the UI when choosing a cloud service or under Settings → Cloud providers.
- No legacy import from `users.duckdb`, `.enc` files, or `migrations.yaml`.
