package dtos

// Excluded segment types
const (
	TypeStandard  = "standard"
	TypeRuleBased = "rule-based"
	TypeLarge     = "large"
)

// RuleBasedSegment
type RuleBasedSegmentDTO struct {
	ChangeNumber    int64                   `json:"changeNumber"`
	Name            string                  `json:"name"`
	Status          string                  `json:"status"`
	TrafficTypeName string                  `json:"trafficTypeName"`
	Excluded        ExcludedDTO             `json:"excluded"`
	Conditions      []RuleBasedConditionDTO `json:"conditions"`
}

type ExcludedDTO struct {
	Keys     []string              `json:"keys"`
	Segments []ExcluededSegmentDTO `json:"segments"`
}

type ExcluededSegmentDTO struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ConditionDTO structure to map a Condition fetched from JSON message.
type RuleBasedConditionDTO struct {
	ConditionType string          `json:"conditionType"`
	MatcherGroup  MatcherGroupDTO `json:"matcherGroup"`
}
