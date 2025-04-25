package strategy

import (
	"github.com/splitio/go-split-commons/v7/storage"
)

// UniqueKeysTracker interface
type UniqueKeysTracker interface {
	Track(featureName string, key string) bool
}

// UniqueKeysTrackerImpl description
type UniqueKeysTrackerImpl struct {
	filter  storage.Filter
	storage storage.UniqueKeysStorageProducer
}

// NewUniqueKeysTracker create new implementation
func NewUniqueKeysTracker(f storage.Filter, storage storage.UniqueKeysStorageProducer) UniqueKeysTracker {
	return &UniqueKeysTrackerImpl{
		filter:  f,
		storage: storage,
	}
}

// Track description
func (t *UniqueKeysTrackerImpl) Track(featureName string, key string) bool {
	fKey := featureName + key
	if t.filter.Contains(fKey) {
		return false
	}

	t.filter.Add(fKey)
	t.storage.Push(featureName, key)

	return true
}
