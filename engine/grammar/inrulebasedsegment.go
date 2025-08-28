package grammar

import (
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
)

// InRuleBasedsegmentMatcher matches if the key passed is in the rule-based segment which the matcher was constructed with
type InRuleBasedSegmentMatcher struct {
	Matcher
	name string
}

// NewInRuleBasedSegmentMatcher instantiates a new InRuleBasedSegmentMatcher
func NewInRuleBasedSegmentMatcher(negate bool, name string, attributeName *string) *InRuleBasedSegmentMatcher {
	return &InRuleBasedSegmentMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		name: name,
	}
}

// Match returns true if the key is in the matcher's rule-based segment
func (m *InRuleBasedSegmentMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	storage, ok := m.Context.Dependency("ruleBasedSegmentStorage").(storage.RuleBasedSegmentStorageConsumer)
	if !ok {
		m.logger.Error("InRuleBasedSegmentMatcher: Unable to retrieve rule-based segment storage!")
		return false
	}

	ruleBasedSegment, err := storage.GetRuleBasedSegmentByName(m.name)
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
			segmentMatcher := NewInSegmentMatcher(false, value.Name, m.attributeName)
			segmentMatcher.Context = m.Context
			segmentMatcher.logger = m.logger
			return segmentMatcher.Match(key, attributes, bucketingKey)

		case dtos.TypeRuleBased:
			ruleBasedSegmentMatcher := NewInRuleBasedSegmentMatcher(false, value.Name, m.attributeName)
			ruleBasedSegmentMatcher.Context = m.Context
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

		conditionMatcher, err := NewRBCondition(&condition, m.Context, m.logger)
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
