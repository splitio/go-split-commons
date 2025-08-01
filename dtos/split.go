package dtos

import (
	"encoding/json"
)

// SplitChangesDTO structure to map JSON message sent by Split servers.
type SplitChangesDTO struct {
	FeatureFlags      FeatureFlagsDTO      `json:"ff"`
	RuleBasedSegments RuleBasedSegmentsDTO `json:"rbs"`
}

type FeatureFlagsDTO struct {
	Since  int64      `json:"s"`
	Till   int64      `json:"t"`
	Splits []SplitDTO `json:"d"`
}

type RuleBasedSegmentsDTO struct {
	Since             int64                 `json:"s"`
	Till              int64                 `json:"t"`
	RuleBasedSegments []RuleBasedSegmentDTO `json:"d"`
}

// SplitDTO structure to map an Split definition fetched from JSON message.
type SplitDTO struct {
	ChangeNumber          int64             `json:"changeNumber"`
	TrafficTypeName       string            `json:"trafficTypeName"`
	Name                  string            `json:"name"`
	TrafficAllocation     int               `json:"trafficAllocation"`
	TrafficAllocationSeed int64             `json:"trafficAllocationSeed"`
	Seed                  int64             `json:"seed"`
	Status                string            `json:"status"`
	Killed                bool              `json:"killed"`
	DefaultTreatment      string            `json:"defaultTreatment"`
	Algo                  int               `json:"algo"`
	Conditions            []ConditionDTO    `json:"conditions"`
	Configurations        map[string]string `json:"configurations"`
	Sets                  []string          `json:"sets"`
	ImpressionsDisabled   bool              `json:"impressionsDisabled"`
}

// MarshalBinary exports SplitDTO to JSON string
func (s SplitDTO) MarshalBinary() (data []byte, err error) {
	return json.Marshal(s)
}

// ConditionDTO structure to map a Condition fetched from JSON message.
type ConditionDTO struct {
	ConditionType string          `json:"conditionType"`
	MatcherGroup  MatcherGroupDTO `json:"matcherGroup"`
	Partitions    []PartitionDTO  `json:"partitions"`
	Label         string          `json:"label"`
}

// PartitionDTO structure to map a Partition definition fetched from JSON message.
type PartitionDTO struct {
	Treatment string `json:"treatment"`
	Size      int    `json:"size"`
}

// MatcherGroupDTO structure to map a Matcher Group definition fetched from JSON message.
type MatcherGroupDTO struct {
	Combiner string       `json:"combiner"`
	Matchers []MatcherDTO `json:"matchers"`
}

// MatcherDTO structure to map a Matcher definition fetched from JSON message.
type MatcherDTO struct {
	KeySelector             *KeySelectorDTO                        `json:"keySelector"`
	MatcherType             string                                 `json:"matcherType"`
	Negate                  bool                                   `json:"negate"`
	UserDefinedSegment      *UserDefinedSegmentMatcherDataDTO      `json:"userDefinedSegmentMatcherData"`
	Whitelist               *WhitelistMatcherDataDTO               `json:"whitelistMatcherData"`
	UnaryNumeric            *UnaryNumericMatcherDataDTO            `json:"unaryNumericMatcherData"`
	Between                 *BetweenMatcherDataDTO                 `json:"betweenMatcherData"`
	BetweenString           *BetweenStringMatcherDataDTO           `json:"betweenStringMatcherData"`
	Dependency              *DependencyMatcherDataDTO              `json:"dependencyMatcherData"`
	Boolean                 *bool                                  `json:"booleanMatcherData"`
	String                  *string                                `json:"stringMatcherData"`
	UserDefinedLargeSegment *UserDefinedLargeSegmentMatcherDataDTO `json:"userDefinedLargeSegmentMatcherData"`
}

// UserDefinedLargeSegmentMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type UserDefinedLargeSegmentMatcherDataDTO struct {
	LargeSegmentName string `json:"largeSegmentName"`
}

// UserDefinedSegmentMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type UserDefinedSegmentMatcherDataDTO struct {
	SegmentName string `json:"segmentName"`
}

// BetweenMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type BetweenMatcherDataDTO struct {
	DataType string `json:"dataType"` //NUMBER or DATETIME
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
}

// BetweenStringMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type BetweenStringMatcherDataDTO struct {
	Start *string `json:"start"`
	End   *string `json:"end"`
}

// UnaryNumericMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type UnaryNumericMatcherDataDTO struct {
	DataType string `json:"dataType"` //NUMBER or DATETIME
	Value    int64  `json:"value"`
}

// WhitelistMatcherDataDTO structure to map a Matcher definition fetched from JSON message.
type WhitelistMatcherDataDTO struct {
	Whitelist []string `json:"whitelist"`
}

// DependencyMatcherDataDTO structure to map matcher definition fetched from JSON message.
type DependencyMatcherDataDTO struct {
	Split      string   `json:"split"`
	Treatments []string `json:"treatments"`
}

// KeySelectorDTO structure to map a Key slector definition fetched from JSON message.
type KeySelectorDTO struct {
	TrafficType string  `json:"trafficType"`
	Attribute   *string `json:"attribute"`
}
