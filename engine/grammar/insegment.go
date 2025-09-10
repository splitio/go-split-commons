package grammar

import (
	"fmt"

	"github.com/splitio/go-split-commons/v6/storage"
)

// InSegmentMatcher matches if the key passed is in the segment which the matcher was constructed with
type InSegmentMatcher struct {
	Matcher
	segmentName    string
	segmentStorage storage.SegmentStorageConsumer
}

// Match returns true if the key is in the matcher's segment
func (m *InSegmentMatcher) Match(key string, attributes map[string]interface{}, bucketingKey *string) bool {

	isInSegment, err := m.segmentStorage.SegmentContainsKey(m.segmentName, key)
	if err != nil {
		m.logger.Error(fmt.Printf("InSegmentMatcher: Segment %s not found", m.segmentName))
	}
	return isInSegment
}

// NewInSegmentMatcher instantiates a new InSegmentMatcher
func NewInSegmentMatcher(negate bool, segmentName string, attributeName *string, segmentStorage storage.SegmentStorageConsumer) *InSegmentMatcher {
	return &InSegmentMatcher{
		Matcher: Matcher{
			negate:        negate,
			attributeName: attributeName,
		},
		segmentName:    segmentName,
		segmentStorage: segmentStorage,
	}
}
