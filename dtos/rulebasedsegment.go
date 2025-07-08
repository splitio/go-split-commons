package dtos

// Excluded segment types
const (
	TypeStandard  = "standard"
	TypeRuleBased = "rule-based"
	TypeLarge     = "large"
)

// RuleBasedSegment
type RuleBasedSegment struct {
	Name            string                  `json:"name"`
	TrafficTypeName string                  `json:"trafficTypeName"`
	ChangeNumber    int64                   `json:"changeNumber"`
	Status          string                  `json:"status"`
	Conditions      []RuleBasedConditionDTO `json:"conditions"`
}

type Excluded struct {
	ExcludedKeys    []string          `json:"excludedKeys"`
	ExcludedSegment []ExcludedSegment `json:"ezcludedSegments"`
}

type ExcludedSegment struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ConditionDTO structure to map a Condition fetched from JSON message.
type RuleBasedConditionDTO struct {
	ConditionType string          `json:"conditionType"`
	MatcherGroup  MatcherGroupDTO `json:"matcherGroup"`
}
