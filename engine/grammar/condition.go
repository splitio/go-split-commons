package grammar

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/datatypes"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Condition struct with added logic that wraps around a DTO
type Condition struct {
	matchers      []MatcherInterface
	combiner      string
	partitions    []Partition
	label         string
	conditionType string
}

// NewCondition instantiates a new Condition struct with appropriate wrappers around dtos and returns it.
func NewCondition(cond *dtos.ConditionDTO, logger logging.LoggerInterface, ruleBuilder RuleBuilder) (*Condition, error) {
	partitions := make([]Partition, 0)
	for _, part := range cond.Partitions {
		partitions = append(partitions, Partition{PartitionData: part})
	}
	matcherObjs, err := processMatchers(cond.MatcherGroup.Matchers, logger, ruleBuilder)
	if err != nil {
		//  At this point the only error forwarded is UnsupportedMatcherError
		return nil, err
	}

	return &Condition{
		combiner:      cond.MatcherGroup.Combiner,
		matchers:      matcherObjs,
		partitions:    partitions,
		label:         cond.Label,
		conditionType: cond.ConditionType,
	}, nil
}

func NewRBCondition(cond *dtos.RuleBasedConditionDTO, logger logging.LoggerInterface, ruleBuilder RuleBuilder) (*Condition, error) {
	partitions := make([]Partition, 0)
	matcherObjs, err := processMatchers(cond.MatcherGroup.Matchers, logger, ruleBuilder)
	if err != nil {
		//  At this point the only error forwarded is UnsupportedMatcherError
		return nil, err
	}

	return &Condition{
		combiner:      cond.MatcherGroup.Combiner,
		matchers:      matcherObjs,
		partitions:    partitions,
		conditionType: cond.ConditionType,
	}, nil
}

func processMatchers(condMatchers []dtos.MatcherDTO, logger logging.LoggerInterface, ruleBuilder RuleBuilder) ([]MatcherInterface, error) {
	matcherObjs := make([]MatcherInterface, 0)
	for _, matcher := range condMatchers {
		m, err := ruleBuilder.BuildMatcher(&matcher)
		if err == nil {
			matcherObjs = append(matcherObjs, m)
		} else {
			logger.Debug("error in BuildMatcher, reason: ", err.Error())
			if _, ok := err.(datatypes.UnsupportedMatcherError); ok {
				return nil, err
			}
		}
	}
	return matcherObjs, nil
}

// Partition struct with added logic that wraps around a DTO
type Partition struct {
	PartitionData dtos.PartitionDTO
}

// ConditionType returns validated condition type. Whitelist by default
func (c *Condition) ConditionType() string {
	switch c.conditionType {
	case ConditionTypeRollout:
		return ConditionTypeRollout
	case ConditionTypeWhitelist:
		return ConditionTypeWhitelist
	default:
		return ConditionTypeWhitelist
	}
}

func (c *Condition) Combiner() string {
	return c.combiner
}

// Label returns the condition's label
func (c *Condition) Label() string {
	return c.label
}

// Matches returns true if the condition matches for a specific key and/or set of attributes
func (c *Condition) Matches(key string, bucketingKey *string, attributes map[string]interface{}) bool {
	partial := make([]bool, len(c.matchers))
	for i, matcher := range c.matchers {
		partial[i] = matcher.Match(key, attributes, bucketingKey)
		if matcher.Negate() {
			partial[i] = !partial[i]
		}
	}
	return applyCombiner(partial, c.combiner)
}

// CalculateTreatment calulates the treatment for a specific condition based on the bucket
func (c *Condition) CalculateTreatment(bucket int) *string {
	accum := 0
	for _, partition := range c.partitions {
		accum += partition.PartitionData.Size
		if bucket <= accum {
			return &partition.PartitionData.Treatment
		}
	}
	return nil
}

func BuildCondition(conditionType string, label string, partitions []Partition, matchers []MatcherInterface, combiner string) *Condition {
	return &Condition{
		conditionType: conditionType,
		label:         label,
		partitions:    partitions,
		matchers:      matchers,
		combiner:      combiner,
	}
}

func applyCombiner(results []bool, combiner string) bool {
	temp := true
	switch combiner {
	case "AND":
		for _, result := range results {
			temp = temp && result
		}
	default:
		return false
	}
	return temp
}
