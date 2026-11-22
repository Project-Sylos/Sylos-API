# Install data encryption

Sylos uses a two-tier key model. Ops Badger directories are **not** encrypted as whole databases; only selected secret fields are sealed with AES-256-GCM.

## Keys

| Key | Storage | Protects |
|-----|---------|----------|
| **Install master key** (32 bytes) | OS keyring only (`Sylos` / `install-master-key`) | Per-migration keys and SFTP saved-host secrets inside `sylos.api/` |
| **Per-migration key** (32 bytes) | Encrypted row `migkey:{id}` in `sylos.api/` (wrapped by install master key) | OAuth / SFTP session credential payloads in `{id}.ops/` (`oauth:{connectionID}` as `enc:v1:…`) |

Startup requires a working OS keyring. If the key cannot be read or stored there, the process exits with a clear error. There is no file or env fallback for the install master key.

## What is encrypted

- Per-migration keys at rest in `sylos.api/` (AES-GCM via install master key)
- SFTP saved-host secrets in `sylos.api/`
- Per-migration OAuth / cloud / SFTP credential JSON rows in `{id}.ops/` (field-level seal via per-migration key)

## What is not encrypted

- Entire `sylos.api/` and `{id}.ops/` Badger files (filesystem permissions apply)
- Catalog / frontier / status / indexes inside ops stores
- Provider OAuth **app** credentials (`client_id` / `client_secret`) in `sylos.api/`
- JWT signing secret (`install_config` in `sylos.api/`)
- In-memory OAuth access tokens

## Export / import

Ops export zips `{id}.ops/` **after removing all `oauth:` rows**. Import strips any leftover `oauth:` keys, mints a new per-migration key under this install’s master key, and flags the migration `needsAttention` when cloud/SFTP sides must be re-authenticated. Local absolute paths may be remapped via the import-only remap endpoint; that does not move credentials.

Do not ship or re-wrap the install master key or `migkey:` blobs as part of a portable export.

## Live FS health

`POST /api/migrations/{id}/fs-health?side=src|dst|both` rehydrates adapters, lists each bound root, then creates and deletes a temporary `.sylos-fs-health-{uuid}/` folder (with an empty file) to verify permissions. A successful probe stamps `lastFsHealthAt` / `lastFsHealthSides` on the migration registry. Resume, start discovery, phase-change, and retry-sweep require a successful check covering the required sides within the last 10 minutes; otherwise they run the same probe synchronously and return the health report instead of starting work. Auth failures may set `needsAttention` with reason `credentials_invalid` (same reconnect path as import).

## Backup and recovery

**Losing the install master key means loss of wrapped per-migration keys and API-side SFTP secrets.** Back up the OS keyring entry for service `Sylos` / user `install-master-key`.

Exported ops zips intentionally omit usable tokens; resume on another install requires re-login to cloud/SFTP providers.

## Operator usage

```bash
./bin/sylos   # requires OS keyring
```

## Validation checklist

Before relying on encryption in production:

- [ ] Stolen `sylos.api/` without master key cannot decrypt migration keys or SFTP secrets
- [ ] Stolen `{id}.ops/` without per-migration key cannot decrypt `oauth:` rows
- [ ] Wrong master key fails fast at startup with a clear error
- [ ] Exported ops zip contains no `oauth:` keys
- [ ] `pkg/tests` scenarios still use plaintext oauth when `tokenEncryptionKey == nil`

## Threat model notes

- Provider OAuth app credentials (`client_id` / `client_secret`) live in the API store. Shipped builds may seed Sylos-owned apps for Dropbox, Box, OneDrive, and SharePoint from Sylos `oauth.env` (`enc:…` values + `oauth.key`, revealed at link time). The committed key is not a secrecy boundary. Google Drive remains bring-your-own-app. Admins can still override under Settings → Cloud providers.
- No legacy import from `users.duckdb`, `.enc` files, or `migrations.yaml`.
