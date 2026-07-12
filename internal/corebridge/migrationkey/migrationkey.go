package migrationkey

import (
	"codeberg.org/Sylos/Sylos-FS/pkg/credentials"
)

// EncryptMigrationKey wraps a per-migration key with the install master key for storage in sylos.duckdb.
func EncryptMigrationKey(plaintext, masterKey []byte) ([]byte, error) {
	return credentials.Encrypt(plaintext, masterKey)
}

// DecryptMigrationKey unwraps a per-migration key stored in sylos.duckdb.
func DecryptMigrationKey(ciphertext, masterKey []byte) ([]byte, error) {
	return credentials.Decrypt(ciphertext, masterKey)
}
