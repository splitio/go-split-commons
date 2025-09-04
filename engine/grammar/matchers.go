package grammar

import (
	"errors"
	"fmt"

	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

var ErrInvalidEqualSemver = errors.New("semver is required for EQUAL_TO_SEMVER matcher type")
var ErrInvalidGTOESemver = errors.New("semver is required for GREATER_THAN_OR_EQUAL_TO_SEMVER matcher type")
var ErrInvalidLTOESemver = errors.New("semver is required for LESS_THAN_OR_EQUAL_TO_SEMVER matcher type")
var ErrInvalidLBetweenSemver = errors.New("semver is required for BETWEEN_SEMVER matcher type")
var ErrInvalidLInListSemver = errors.New("semver is required for IN_LIST_SEMVER matcher type")
var GoClientFeatureFlagsRules = []string{MatcherTypeAllKeys, MatcherTypeInSegment, MatcherTypeWhitelist, MatcherTypeEqualTo, MatcherTypeGreaterThanOrEqualTo, MatcherTypeLessThanOrEqualTo, MatcherTypeBetween,
	MatcherTypeEqualToSet, MatcherTypePartOfSet, MatcherTypeContainsAllOfSet, MatcherTypeContainsAnyOfSet, MatcherTypeStartsWith, MatcherTypeEndsWith, MatcherTypeContainsString, MatcherTypeInSplitTreatment,
	MatcherTypeEqualToBoolean, MatcherTypeMatchesString, MatcherEqualToSemver, MatcherTypeGreaterThanOrEqualToSemver, MatcherTypeLessThanOrEqualToSemver, MatcherTypeBetweenSemver, MatcherTypeInListSemver,
	MatcherTypeInRuleBasedSegment}
var GoClientRuleBasedSegmentRules = []string{MatcherTypeAllKeys, MatcherTypeInSegment, MatcherTypeWhitelist, MatcherTypeEqualTo, MatcherTypeGreaterThanOrEqualTo, MatcherTypeLessThanOrEqualTo, MatcherTypeBetween,
	MatcherTypeEqualToSet, MatcherTypePartOfSet, MatcherTypeContainsAllOfSet, MatcherTypeContainsAnyOfSet, MatcherTypeStartsWith, MatcherTypeEndsWith, MatcherTypeContainsString,
	MatcherTypeEqualToBoolean, MatcherTypeMatchesString, MatcherEqualToSemver, MatcherTypeGreaterThanOrEqualToSemver, MatcherTypeLessThanOrEqualToSemver, MatcherTypeBetweenSemver, MatcherTypeInListSemver,
	MatcherTypeInRuleBasedSegment}
var SyncProxyFeatureFlagsRules = []string{MatcherTypeAllKeys, MatcherTypeInSegment, MatcherTypeWhitelist, MatcherTypeEqualTo, MatcherTypeGreaterThanOrEqualTo, MatcherTypeLessThanOrEqualTo, MatcherTypeBetween,
	MatcherTypeEqualToSet, MatcherTypePartOfSet, MatcherTypeContainsAllOfSet, MatcherTypeContainsAnyOfSet, MatcherTypeStartsWith, MatcherTypeEndsWith, MatcherTypeContainsString, MatcherTypeInSplitTreatment,
	MatcherTypeEqualToBoolean, MatcherTypeMatchesString, MatcherEqualToSemver, MatcherTypeGreaterThanOrEqualToSemver, MatcherTypeLessThanOrEqualToSemver, MatcherTypeBetweenSemver, MatcherTypeInListSemver, MatcherTypeInLargeSegment,
	MatcherTypeInRuleBasedSegment}
var SyncProxyRuleBasedSegmentRules = []string{MatcherTypeAllKeys, MatcherTypeInSegment, MatcherTypeWhitelist, MatcherTypeEqualTo, MatcherTypeGreaterThanOrEqualTo, MatcherTypeLessThanOrEqualTo, MatcherTypeBetween,
	MatcherTypeEqualToSet, MatcherTypePartOfSet, MatcherTypeContainsAllOfSet, MatcherTypeContainsAnyOfSet, MatcherTypeStartsWith, MatcherTypeEndsWith, MatcherTypeContainsString,
	MatcherTypeEqualToBoolean, MatcherTypeMatchesString, MatcherEqualToSemver, MatcherTypeGreaterThanOrEqualToSemver, MatcherTypeLessThanOrEqualToSemver, MatcherTypeBetweenSemver, MatcherTypeInListSemver, MatcherTypeInLargeSegment,
	MatcherTypeInRuleBasedSegment}

const (
	// MatcherTypeAllKeys string value
	MatcherTypeAllKeys = "ALL_KEYS"
	// MatcherTypeInSegment string value
	MatcherTypeInSegment = "IN_SEGMENT"
	// MatcherTypeWhitelist string value
	MatcherTypeWhitelist = "WHITELIST"
	// MatcherTypeEqualTo string value
	MatcherTypeEqualTo = "EQUAL_TO"
	// MatcherTypeGreaterThanOrEqualTo string value
	MatcherTypeGreaterThanOrEqualTo = "GREATER_THAN_OR_EQUAL_TO"
	// MatcherTypeLessThanOrEqualTo string value
	MatcherTypeLessThanOrEqualTo = "LESS_THAN_OR_EQUAL_TO"
	// MatcherTypeBetween string value
	MatcherTypeBetween = "BETWEEN"
	// MatcherTypeEqualToSet string value
	MatcherTypeEqualToSet = "EQUAL_TO_SET"
	// MatcherTypePartOfSet string value
	MatcherTypePartOfSet = "PART_OF_SET"
	// MatcherTypeContainsAllOfSet string value
	MatcherTypeContainsAllOfSet = "CONTAINS_ALL_OF_SET"
	// MatcherTypeContainsAnyOfSet string value
	MatcherTypeContainsAnyOfSet = "CONTAINS_ANY_OF_SET"
	// MatcherTypeStartsWith string value
	MatcherTypeStartsWith = "STARTS_WITH"
	// MatcherTypeEndsWith string value
	MatcherTypeEndsWith = "ENDS_WITH"
	// MatcherTypeContainsString string value
	MatcherTypeContainsString = "CONTAINS_STRING"
	// MatcherTypeInSplitTreatment string value
	MatcherTypeInSplitTreatment = "IN_SPLIT_TREATMENT"
	// MatcherTypeEqualToBoolean string value
	MatcherTypeEqualToBoolean = "EQUAL_TO_BOOLEAN"
	// MatcherTypeMatchesString string value
	MatcherTypeMatchesString = "MATCHES_STRING"
	// MatcherEqualToSemver string value
	MatcherEqualToSemver = "EQUAL_TO_SEMVER"
	// MatcherTypeGreaterThanOrEqualToSemver string value
	MatcherTypeGreaterThanOrEqualToSemver = "GREATER_THAN_OR_EQUAL_TO_SEMVER"
	// MatcherTypeLessThanOrEqualToSemver string value
	MatcherTypeLessThanOrEqualToSemver = "LESS_THAN_OR_EQUAL_TO_SEMVER"
	// MatcherTypeBetweenSemver string value
	MatcherTypeBetweenSemver = "BETWEEN_SEMVER"
	// MatcherTypeInListSemver string value
	MatcherTypeInListSemver = "IN_LIST_SEMVER"
	// MatcherInLargeSegment string value
	MatcherTypeInLargeSegment = "IN_LARGE_SEGMENT"
	// MatcherInRuleBasedSegment string value
	MatcherTypeInRuleBasedSegment = "IN_RULE_BASED_SEGMENT"
)

// MatcherInterface should be implemented by all matchers
type MatcherInterface interface {
	Match(key string, attributes map[string]interface{}, bucketingKey *string) bool
	Negate() bool
	base() *Matcher // This method is used to return the embedded matcher when iterating over interfaces
	matchingKey(key string, attributes map[string]interface{}) (interface{}, error)
}

// Matcher struct with added logic that wraps around a DTO
type Matcher struct {
	*injection.Context
	negate        bool
	attributeName *string
	logger        logging.LoggerInterface
}

// Negate returns whether this mather is negated or not
func (m *Matcher) Negate() bool {
	return m.negate
}

func (m *Matcher) matchingKey(key string, attributes map[string]interface{}) (interface{}, error) {
	if m.attributeName == nil {
		return key, nil
	}

	// Reaching this point means WE NEED attributes
	if attributes == nil {
		return nil, errors.New("Attribute required but no attributes provided")
	}

	attrValue, found := attributes[*m.attributeName]
	if !found {
		return nil, fmt.Errorf(
			"Attribute \"%s\" required but not present in provided attribute map",
			*m.attributeName,
		)
	}

	return attrValue, nil
}

// matcher returns the matcher instance embbeded in structs
func (m *Matcher) base() *Matcher {
	return m
}
