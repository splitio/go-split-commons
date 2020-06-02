package mutexmap

import (
	"sync"

	"github.com/splitio/go-toolkit/datastructures/set"
)

// MMSegmentStorage contains is an in-memory implementation of segment storage
type MMSegmentStorage struct {
	data      map[string]*set.ThreadUnsafeSet
	till      map[string]int64
	mutex     *sync.RWMutex
	tillMutex *sync.RWMutex
}

// NewMMSegmentStorage instantiates a new MMSegmentStorage
func NewMMSegmentStorage() *MMSegmentStorage {
	return &MMSegmentStorage{
		data:      make(map[string]*set.ThreadUnsafeSet),
		till:      make(map[string]int64),
		mutex:     &sync.RWMutex{},
		tillMutex: &sync.RWMutex{},
	}
}

// Clear replaces the segment storage with an empty one.
func (m *MMSegmentStorage) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data = make(map[string]*set.ThreadUnsafeSet)
}

// ChangeNumber returns the latest timestamp the segment was fetched
func (m *MMSegmentStorage) ChangeNumber(segmentName string) (int64, error) {
	m.tillMutex.RLock()
	defer m.tillMutex.RUnlock()
	return m.till[segmentName], nil
}

// Keys retrieves a segment from the in-memory storage
// NOTE: A pointer TO A COPY is returned, in order to avoid race conditions between
// evaluations and sdk <-> backend sync
func (m *MMSegmentStorage) Keys(segmentName string) *set.ThreadUnsafeSet {
	// @TODO replace to IsInSegment
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	item, exists := m.data[segmentName]
	if !exists {
		return nil
	}
	s := item.Copy().(*set.ThreadUnsafeSet)
	return s
}

// SetChangeNumber sets the till value belong to segmentName
func (m *MMSegmentStorage) SetChangeNumber(name string, till int64) error {
	m.tillMutex.Lock()
	defer m.tillMutex.Unlock()
	m.till[name] = till
	return nil
}

// Update adds a new segment to the in-memory storage
func (m *MMSegmentStorage) Update(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, till int64) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, ok := m.data[name]
	if !ok {
		m.data[name] = set.NewSet()
	}
	if !toRemove.IsEmpty() {
		m.data[name].Remove(toRemove.List()...)
	}
	if !toAdd.IsEmpty() {
		m.data[name].Add(toAdd.List()...)
	}
	m.SetChangeNumber(name, till)
	return nil
}
