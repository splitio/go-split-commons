package dtos

// Excluded segment types
const (
	TypeStandard  = "standard"
	TypeRuleBased = "rule-based"
	TypeLarge     = "large"
)

// RuleBasedSegment
type RuleBasedSegment struct {
	Name            string
	TrafficTypeName string
	ChangeNumber    int64
	Status          string
	Conditions      []RuleBasedConditionDTO
}

type Excluded struct {
	ExcludedKeys    []string
	ExcludedSegment []ExcludedSegment
}

type ExcludedSegment struct {
	Name string
	Type string
}

// ConditionDTO structure to map a Condition fetched from JSON message.
type RuleBasedConditionDTO struct {
	ConditionType string
	MatcherGroup  MatcherGroupDTO
}
