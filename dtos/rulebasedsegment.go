package dtos

// Excluded segment types
const (
	TypeStandard = "standard";
  	TypeRuleBased = "rule-based";
  	TypeLarge = "large";
)

// RuleBasedSegment
type RuleBasedSegment struct {
	Name            string
	TrafficTypeName string
	ChangeNumber    int64
	Status          Status
	Conditions      []ConditionDTO
	
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
type ConditionDTO struct {
	ConditionType string         
	MatcherGroup  MatcherGroupDTO
}

// RFD struct
type RFD struct {
	Data   Data   `json:"d"`
	Params Params `json:"p"`
}

// RuleBasedSegmentRFDResponseDTO
type RuleBasedSegmentRFDResponseDTO struct {
	Name             string `json:"n"`
	NotificationType string `json:"t"`
	RFD              *RFD   `json:"rfd,omitempty"`
	SpecVersion      string `json:"v"`
	ChangeNumber     int64  `json:"cn"`
}