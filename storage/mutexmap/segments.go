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

// Put adds a new segment to the in-memory storage
func (m *MMSegmentStorage) Put(name string, segment *set.ThreadUnsafeSet, till int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[name] = segment
	m.SetChangeNumber(name, till)
}

// ChangeNumber returns the latest timestamp the segment was fetched
func (m *MMSegmentStorage) ChangeNumber(segmentName string) (int64, error) {
	m.tillMutex.RLock()
	defer m.tillMutex.RUnlock()
	return m.till[segmentName], nil
}

// SetChangeNumber sets the till value belong to segmentName
func (m *MMSegmentStorage) SetChangeNumber(name string, till int64) error {
	m.tillMutex.Lock()
	defer m.tillMutex.Unlock()
	m.till[name] = till
	return nil
}

func (m *MMSegmentStorage) _removeTill(segmentName string) {
	m.tillMutex.Lock()
	defer m.tillMutex.Unlock()
	delete(m.till, segmentName)
}

// Remove deletes a segment from the in-memmory storage
func (m *MMSegmentStorage) Remove(segmentName string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.data, segmentName)
	m._removeTill(segmentName)
}

// Clear replaces the segment storage with an empty one.
func (m *MMSegmentStorage) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data = make(map[string]*set.ThreadUnsafeSet)
}

// Get retrieves a segment from the in-memory storage
// NOTE: A pointer TO A COPY is returned, in order to avoid race conditions between
// evaluations and sdk <-> backend sync
func (m *MMSegmentStorage) Get(segmentName string) *set.ThreadUnsafeSet {
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
