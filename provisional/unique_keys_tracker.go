package provisional

import (
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/datastructures/set"
)

// UniqueKeysTracker interface
type UniqueKeysTracker interface {
	Track(featureName string, key string) bool
}

// UniqueKeysTrackerImpl description
type UniqueKeysTrackerImpl struct {
	filterAdapter storage.FilterAdpter
	cache         map[string]*set.ThreadUnsafeSet
}

// NewUniqueKeysTracker create new implementation
func NewUniqueKeysTracker(adapter storage.FilterAdpter) UniqueKeysTracker {
	return &UniqueKeysTrackerImpl{
		filterAdapter: adapter,
		cache:         make(map[string]*set.ThreadUnsafeSet),
	}
}

// Track description
func (t *UniqueKeysTrackerImpl) Track(featureName string, key string) bool {
	if t.filterAdapter.Contains(featureName, key) {
		return false
	}

	t.filterAdapter.Add(featureName, key)

	_, ok := t.cache[featureName]
	if !ok {
		t.cache[featureName] = set.NewSet()
	}

	t.cache[featureName].Add(key)

	return true
}
