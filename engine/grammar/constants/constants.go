package constants

const (
	// SplitStatusActive represents an active feature flag
	SplitStatusActive = "ACTIVE"
	// SplitStatusArchived represents an archived feature flag
	SplitStatusArchived = "ARCHIVED"

	// SplitAlgoLegacy represents the legacy implementation of hash function for bucketing
	SplitAlgoLegacy = 1

	// SplitAlgoMurmur represents the murmur implementation of the hash funcion for bucketing
	SplitAlgoMurmur = 2
)
