package matchers

import (
	"fmt"

	"github.com/splitio/go-toolkit/v5/injection"
)

var keyExists = struct{}{}

// WhitelistMatcher matches if the key received is present in the matcher's whitelist
type WhitelistMatcher struct {
	Matcher
	whitelist map[string]struct{}
}

// Match returns true if the key is present in the whitelist.
func (m *WhitelistMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string, ctx *injection.Context) bool {
	matchingKey, err := m.matchingKey(key, attributes)
	if err != nil {
		m.logger.Warning(fmt.Sprintf("WhitelistMatcher: %s", err.Error()))
		return false
	}

	stringMatchingKey, ok := matchingKey.(string)
	if !ok {
		m.logger.Error("WhitelistMatcher: Cannot type-assert key to string")
		return false
	}

	_, has := m.whitelist[stringMatchingKey]
	return has
}

// NewWhitelistMatcher returns a new WhitelistMatcher
func NewWhitelistMatcher(negate bool, whitelist []string, attributeName *string) *WhitelistMatcher {
	wlSet := make(map[string]struct{}, len(whitelist))
	for _, elem := range whitelist {
		wlSet[elem] = keyExists
	}
	return &WhitelistMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		whitelist: wlSet,
	}
}
