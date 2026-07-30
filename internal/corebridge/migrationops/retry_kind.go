package migrationops

// RetryKind identifies discovery vs copy retry mutations in path review.
type RetryKind string

const (
	RetryKindDiscovery RetryKind = "discovery"
	RetryKindCopy      RetryKind = "copy"
	RetryKindDelete    RetryKind = "delete"
)
