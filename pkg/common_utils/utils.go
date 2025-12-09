package common_utils

import "github.com/Project-Sylos/Migration-Engine/pkg/db"


// hashPath hashes a path using SHA256 and returns the key format used by the migration engine
func HashPath(path string) string {
	return db.HashPath(path)
}