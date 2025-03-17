package strategy

import (
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/storage/inmemory/mutexmap"
)

// UniqueKeysTracker interface
type UniqueKeysTracker interface {
	Track(featureName string, key string) bool
}

// UniqueKeysTrackerImpl description
type UniqueKeysTrackerImpl struct {
	filter storage.Filter
	cache  *mutexmap.MMUniqueKeysStorage
}

// NewUniqueKeysTracker create new implementation
func NewUniqueKeysTracker(f storage.Filter, cache *mutexmap.MMUniqueKeysStorage) UniqueKeysTracker {
	return &UniqueKeysTrackerImpl{
		filter: f,
		cache:  cache,
	}
}

// Track description
func (t *UniqueKeysTrackerImpl) Track(featureName string, key string) bool {
	fKey := featureName + key
	if t.filter.Contains(fKey) {
		return false
	}

	t.filter.Add(fKey)
	t.cache.Add(featureName, key)

	return true
}
