package condition

const (
	// ConditionTypeWhitelist represents a normal condition
	ConditionTypeWhitelist = "WHITELIST"
	// ConditionTypeRollout represents a condition that will return default if traffic allocatio is exceeded
	ConditionTypeRollout = "ROLLOUT"

	// MatcherCombinerAnd represents that all matchers in the group are required
	MatcherCombinerAnd = 0
)
