package condition

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers"
	"github.com/splitio/go-split-commons/v6/engine/grammar/matchers/datatypes"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Condition struct with added logic that wraps around a DTO
type Condition struct {
	matchers      []matchers.MatcherInterface
	combiner      string
	partitions    []Partition
	label         string
	conditionType string
}

// NewCondition instantiates a new Condition struct with appropriate wrappers around dtos and returns it.
func NewCondition(cond *dtos.ConditionDTO, ctx *injection.Context, logger logging.LoggerInterface) (*Condition, error) {
	partitions := make([]Partition, 0)
	for _, part := range cond.Partitions {
		partitions = append(partitions, Partition{PartitionData: part})
	}
	matcherObjs, err := processMatchers(cond.MatcherGroup.Matchers, ctx, logger)
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

func processMatchers(condMatchers []dtos.MatcherDTO, ctx *injection.Context, logger logging.LoggerInterface) ([]matchers.MatcherInterface, error) {
	matcherObjs := make([]matchers.MatcherInterface, 0)
	for _, matcher := range condMatchers {
		m, err := matchers.BuildMatcher(&matcher, ctx, logger)
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

func BuildCondition(conditionType string, label string, partitions []Partition, matchers []matchers.MatcherInterface, combiner string) Condition {
	conditionReplacementUnsupportedMatcher := Condition{
		conditionType: conditionType,
		label:         label,
		partitions:    partitions,
		matchers:      matchers,
		combiner:      combiner,
	}
	return conditionReplacementUnsupportedMatcher
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
