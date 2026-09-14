package apidb

import (
	"fmt"
	"strings"
)

const (
	prefixCfg           = "cfg:"
	prefixUser          = "user:"
	prefixUserName      = "user:name:"
	prefixUserAudit     = "user:audit:"
	prefixMig           = "mig:"
	prefixMigKey        = "migkey:"
	prefixOAuth         = "oauth:"
	prefixSftpKnown     = "sftp:known:"
	prefixSftpSaved     = "sftp:saved:"
	prefixSftpSavedEp   = "sftp:saved:ep:"
	prefixScale         = "scale:"
	prefixRuleset       = "ruleset:"
	keyMetaSchemaVer    = "meta:schema_version"
	defaultStoreDirName = "sylos.api"
	schemaVersion       = 1
)

func keyCfg(k string) []byte {
	return []byte(prefixCfg + k)
}

func keyUser(id string) []byte {
	return []byte(prefixUser + id)
}

func keyUserName(username string) []byte {
	return []byte(prefixUserName + strings.ToLower(strings.TrimSpace(username)))
}

func keyUserAudit(id string) []byte {
	return []byte(prefixUserAudit + id)
}

func keyMig(id string) []byte {
	return []byte(prefixMig + id)
}

func keyMigKey(id string) []byte {
	return []byte(prefixMigKey + id)
}

func keyOAuth(providerID string) []byte {
	return []byte(prefixOAuth + providerID)
}

func keySftpKnown(hostPort string) []byte {
	return []byte(prefixSftpKnown + hostPort)
}

func keySftpSaved(id string) []byte {
	return []byte(prefixSftpSaved + id)
}

func keySftpSavedEp(host string, port int, username string) []byte {
	host = strings.TrimSpace(strings.ToLower(host))
	username = strings.TrimSpace(username)
	if port <= 0 {
		port = 22
	}
	return []byte(fmt.Sprintf("%s%s:%d:%s", prefixSftpSavedEp, host, port, username))
}

func keyScale(scope, scopeKey, mode string) []byte {
	return []byte(prefixScale + scope + ":" + scopeKey + ":" + mode)
}

func keyScalePrefix(scope, scopeKey string) []byte {
	return []byte(prefixScale + scope + ":" + scopeKey + ":")
}

func keyRuleset(id string) []byte {
	return []byte(prefixRuleset + id)
}

func isIndexKey(key []byte) bool {
	s := string(key)
	return strings.HasPrefix(s, prefixUserName) ||
		strings.HasPrefix(s, prefixUserAudit) ||
		strings.HasPrefix(s, prefixSftpSavedEp) ||
		s == keyMetaSchemaVer
}
