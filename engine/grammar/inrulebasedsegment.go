package grammar

import (
	"fmt"

	"github.com/splitio/go-split-commons/v9/dtos"
	"golang.org/x/exp/slices"
)

// InRuleBasedsegmentMatcher matches if the key passed is in the rule-based segment which the matcher was constructed with
type InRuleBasedSegmentMatcher struct {
	Matcher
	name        string
	ruleBuilder RuleBuilder
}

// NewInRuleBasedSegmentMatcher instantiates a new InRuleBasedSegmentMatcher
func NewInRuleBasedSegmentMatcher(negate bool, name string, attributeName *string, ruleBuilder RuleBuilder) *InRuleBasedSegmentMatcher {
	return &InRuleBasedSegmentMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		name:        name,
		ruleBuilder: ruleBuilder,
	}
}

// Match returns true if the key is in the matcher's rule-based segment
func (m *InRuleBasedSegmentMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {

	ruleBasedSegment, err := m.ruleBuilder.ruleBasedSegmentStorage.GetRuleBasedSegmentByName(m.name)
	if err != nil {
		m.logger.Error(fmt.Printf("InRuleBasedSegmentMatcher: Rule-based Segment %s not found", m.name))
	}
	if m.keyIsExcluded(*ruleBasedSegment, key) {
		return false
	}
	if m.inExcludedSegment(*ruleBasedSegment, key, attributes, bucketingKey) {
		return false
	}

	return m.matchesConditions(*ruleBasedSegment, key, attributes, bucketingKey)
}

func (m *InRuleBasedSegmentMatcher) keyIsExcluded(ruleBasedSegment dtos.RuleBasedSegmentDTO, key string) bool {
	return slices.Contains(ruleBasedSegment.Excluded.Keys, key)
}

func (m *InRuleBasedSegmentMatcher) inExcludedSegment(ruleBasedSegment dtos.RuleBasedSegmentDTO, key string, attributes map[string]interface{}, bucketingKey *string) bool {
	for _, value := range ruleBasedSegment.Excluded.Segments {
		switch value.Type {
		case dtos.TypeStandard:
			segmentMatcher := NewInSegmentMatcher(false, value.Name, m.attributeName, m.ruleBuilder.segmentStorage)
			segmentMatcher.logger = m.logger
			return segmentMatcher.Match(key, attributes, bucketingKey)

		case dtos.TypeRuleBased:
			ruleBasedSegmentMatcher := NewInRuleBasedSegmentMatcher(false, value.Name, m.attributeName, m.ruleBuilder)
			ruleBasedSegmentMatcher.logger = m.logger
			if ruleBasedSegmentMatcher.Match(key, attributes, bucketingKey) {
				return true
			}
		}
	}
	return false
}

func (m *InRuleBasedSegmentMatcher) matchesConditions(ruleBasedSegment dtos.RuleBasedSegmentDTO, key string, attributes map[string]interface{}, bucketingKey *string) bool {
	for _, condition := range ruleBasedSegment.Conditions {

		conditionMatcher, err := NewRBCondition(&condition, m.logger, m.ruleBuilder)
		if err != nil {
			m.logger.Error(fmt.Printf("InRuleBasedSegmentMatcher: error creating new condition"))
			return false
		}
		if conditionMatcher.Matches(key, bucketingKey, attributes) {
			return true
		}
	}
	return false
}
