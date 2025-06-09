package matchers

import (
	"fmt"

	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/injection"
)

// InLargeSegmentMatcher matches if the key passed is in the large segment which the matcher was constructed with
type InLargeSegmentMatcher struct {
	Matcher
	name string
}

// Match returns true if the key is in the matcher's segment
func (m *InLargeSegmentMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string, ctx *injection.Context) bool {
	storage, ok := ctx.Dependency("largeSegmentStorage").(storage.LargeSegmentStorageConsumer)
	if !ok {
		m.logger.Error("InLargeSegmentMatcher: Unable to retrieve large segment storage!")
		return false
	}

	isInLargeSegment, err := storage.IsInLargeSegment(m.name, key)
	if err != nil {
		m.logger.Error(fmt.Printf("InLargeSegmentMatcher: Large Segment %s not found", m.name))
	}
	return isInLargeSegment
}

// NewInLargeSegmentMatcher instantiates a new InLargeSegmentMatcher
func NewInLargeSegmentMatcher(negate bool, name string, attributeName *string) *InLargeSegmentMatcher {
	return &InLargeSegmentMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		name: name,
	}
}
