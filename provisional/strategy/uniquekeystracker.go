package strategy

import (
	"sync"

	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
)

// UniqueKeysTracker interface
type UniqueKeysTracker interface {
	Track(featureName string, key string) bool
	PopAll() map[string]*set.ThreadUnsafeSet
}

// UniqueKeysTrackerImpl description
type UniqueKeysTrackerImpl struct {
	filter storage.Filter
	cache  map[string]*set.ThreadUnsafeSet
	mutex  *sync.RWMutex
}

// NewUniqueKeysTracker create new implementation
func NewUniqueKeysTracker(f storage.Filter) UniqueKeysTracker {
	return &UniqueKeysTrackerImpl{
		filter: f,
		cache:  make(map[string]*set.ThreadUnsafeSet),
		mutex:  &sync.RWMutex{},
	}
}

// Track description
func (t *UniqueKeysTrackerImpl) Track(featureName string, key string) bool {
	fKey := featureName + key
	if t.filter.Contains(fKey) {
		return false
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.filter.Add(fKey)
	_, ok := t.cache[featureName]
	if !ok {
		t.cache[featureName] = set.NewSet()
	}

	t.cache[featureName].Add(key)

	return true
}

// PopAll returns all the elements stored in the cache and resets the cache
func (t *UniqueKeysTrackerImpl) PopAll() map[string]*set.ThreadUnsafeSet {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	toReturn := t.cache
	t.cache = make(map[string]*set.ThreadUnsafeSet)

	return toReturn
}
