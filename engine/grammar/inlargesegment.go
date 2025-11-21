package grammar

import (
	"fmt"

	"github.com/splitio/go-split-commons/v9/storage"
)

// InLargeSegmentMatcher matches if the key passed is in the large segment which the matcher was constructed with
type InLargeSegmentMatcher struct {
	Matcher
	name                string
	largeSegmentStorage storage.LargeSegmentStorageConsumer
}

// Match returns true if the key is in the matcher's segment
func (m *InLargeSegmentMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {
	isInLargeSegment, err := m.largeSegmentStorage.IsInLargeSegment(m.name, key)
	if err != nil {
		m.logger.Error(fmt.Printf("InLargeSegmentMatcher: Large Segment %s not found", m.name))
	}
	return isInLargeSegment
}

// NewInLargeSegmentMatcher instantiates a new InLargeSegmentMatcher
func NewInLargeSegmentMatcher(negate bool, name string, attributeName *string, largeSegmentStorage storage.LargeSegmentStorageConsumer) *InLargeSegmentMatcher {
	return &InLargeSegmentMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		name:                name,
		largeSegmentStorage: largeSegmentStorage,
	}
}
