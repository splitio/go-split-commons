package grammar

import (
	"errors"
	"fmt"

	"github.com/splitio/go-toolkit/v5/logging"
)

var ErrInvalidEqualSemver = errors.New("semver is required for EQUAL_TO_SEMVER matcher type")
var ErrInvalidGTOESemver = errors.New("semver is required for GREATER_THAN_OR_EQUAL_TO_SEMVER matcher type")
var ErrInvalidLTOESemver = errors.New("semver is required for LESS_THAN_OR_EQUAL_TO_SEMVER matcher type")
var ErrInvalidLBetweenSemver = errors.New("semver is required for BETWEEN_SEMVER matcher type")
var ErrInvalidLInListSemver = errors.New("semver is required for IN_LIST_SEMVER matcher type")

// MatcherInterface should be implemented by all matchers
type MatcherInterface interface {
	Match(key string, attributes map[string]interface{}, bucketingKey *string) bool
	Negate() bool
	base() *Matcher // This method is used to return the embedded matcher when iterating over interfaces
	matchingKey(key string, attributes map[string]interface{}) (interface{}, error)
}

// Matcher struct with added logic that wraps around a DTO
type Matcher struct {
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
