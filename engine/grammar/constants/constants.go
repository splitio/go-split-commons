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

const (
	// MatcherTypeAllKeys string value
	MatcherTypeAllKeys = "ALL_KEYS"
	// MatcherTypeInSegment string value
	MatcherTypeInSegment = "IN_SEGMENT"
	// MatcherTypeWhitelist string value
	MatcherTypeWhitelist = "WHITELIST"
	// MatcherTypeEqualTo string value
	MatcherTypeEqualTo = "EQUAL_TO"
	// MatcherTypeGreaterThanOrEqualTo string value
	MatcherTypeGreaterThanOrEqualTo = "GREATER_THAN_OR_EQUAL_TO"
	// MatcherTypeLessThanOrEqualTo string value
	MatcherTypeLessThanOrEqualTo = "LESS_THAN_OR_EQUAL_TO"
	// MatcherTypeBetween string value
	MatcherTypeBetween = "BETWEEN"
	// MatcherTypeEqualToSet string value
	MatcherTypeEqualToSet = "EQUAL_TO_SET"
	// MatcherTypePartOfSet string value
	MatcherTypePartOfSet = "PART_OF_SET"
	// MatcherTypeContainsAllOfSet string value
	MatcherTypeContainsAllOfSet = "CONTAINS_ALL_OF_SET"
	// MatcherTypeContainsAnyOfSet string value
	MatcherTypeContainsAnyOfSet = "CONTAINS_ANY_OF_SET"
	// MatcherTypeStartsWith string value
	MatcherTypeStartsWith = "STARTS_WITH"
	// MatcherTypeEndsWith string value
	MatcherTypeEndsWith = "ENDS_WITH"
	// MatcherTypeContainsString string value
	MatcherTypeContainsString = "CONTAINS_STRING"
	// MatcherTypeInSplitTreatment string value
	MatcherTypeInSplitTreatment = "IN_SPLIT_TREATMENT"
	// MatcherTypeEqualToBoolean string value
	MatcherTypeEqualToBoolean = "EQUAL_TO_BOOLEAN"
	// MatcherTypeMatchesString string value
	MatcherTypeMatchesString = "MATCHES_STRING"
	// MatcherEqualToSemver string value
	MatcherEqualToSemver = "EQUAL_TO_SEMVER"
	// MatcherTypeGreaterThanOrEqualToSemver string value
	MatcherTypeGreaterThanOrEqualToSemver = "GREATER_THAN_OR_EQUAL_TO_SEMVER"
	// MatcherTypeLessThanOrEqualToSemver string value
	MatcherTypeLessThanOrEqualToSemver = "LESS_THAN_OR_EQUAL_TO_SEMVER"
	// MatcherTypeBetweenSemver string value
	MatcherTypeBetweenSemver = "BETWEEN_SEMVER"
	// MatcherTypeInListSemver string value
	MatcherTypeInListSemver = "IN_LIST_SEMVER"
	// MatcherInLargeSegment string value
	MatcherTypeInLargeSegment = "IN_LARGE_SEGMENT"
	// MatcherInRuleBasedSegment string value
	MatcherTypeInRuleBasedSegment = "IN_RULE_BASED_SEGMENT"
)
