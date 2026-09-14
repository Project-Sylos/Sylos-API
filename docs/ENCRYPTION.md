# Install data encryption

Sylos uses a two-tier encryption model for data at rest.

## Keys

| Key | Storage | Protects |
|-----|---------|----------|
| **Install master key** (32 bytes) | OS keyring (`Sylos` / `install-master-key`) by default, or `creds/.env` with `--use-env-keys` | `sylos.api/` Badger store (users, migration registry, per-migration keys, provider OAuth app credentials) |
| **Per-migration key** (32 bytes) | Encrypted row in `sylos.api/` | Each migration engine store (ops Badger + catalog DuckDB on ME branch) |

Default mode requires a working OS keyring; startup fails if the key cannot be read or stored there.

With `--use-env-keys`, `SYLOS_MASTER_KEY` overrides `creds/.env`. Setting `SYLOS_MASTER_KEY` or `SYLOS_KEY_FILE` without `--use-env-keys` is rejected at startup.

There is no plaintext fallback file for the install master key in default mode.

## What is encrypted

- Per-migration encryption keys at rest in `sylos.api/` (AES-GCM via install master key)
- SFTP saved-host secrets in `sylos.api/`
- Per-migration engine stores when opened through the API (ME catalog DuckDB + ops Badger)
- OAuth refresh tokens are stored as plaintext JSON rows inside encrypted migration DBs (not field-level AES)

## What is not encrypted

- JWT signing secret (`install_config` in `sylos.api/`, or `SYLOS_JWT_SECRET` / `jwt.secret` in config)
- In-memory OAuth access tokens
- Badger value logs for `sylos.api/` (filesystem permissions apply)

## Backup and recovery

**Losing the install master key means total loss of `sylos.api/` and all per-migration keys.** Back up one of:

- OS keyring export for service `Sylos` / user `install-master-key`
- `creds/.env` when using `--use-env-keys`
- `creds/.env` or `SYLOS_MASTER_KEY` when using `--use-env-keys`

Per-migration engine data is useless without both the on-disk folders and the corresponding key from the API store.

## Operator flags

```bash
./bin/sylos                  # default: keyring
./bin/sylos --use-env-keys   # read/write SYLOS_MASTER_KEY in creds/.env
```

## Validation checklist

Before relying on encryption in production:

- [ ] Stolen `sylos.api/` without master key cannot decrypt migration keys or SFTP secrets
- [ ] Stolen migration data without per-migration key is unreadable
- [ ] Wrong master key fails fast at startup with a clear error
- [ ] `pkg/tests` scenarios still use plaintext DBs (`EncryptionKey == nil`)

## Threat model notes

- Provider OAuth app credentials (`client_id` / `client_secret`) live in the API store; configure in the UI when choosing a cloud service or under Settings → Cloud providers.
- No legacy import from `users.duckdb`, `.enc` files, or `migrations.yaml`.
