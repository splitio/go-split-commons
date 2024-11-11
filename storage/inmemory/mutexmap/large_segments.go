package mutexmap

import (
	"sort"
	"sync"

	"github.com/splitio/go-split-commons/v6/storage"
)

// LargeSegmentsStorageImpl implements the LargeSegmentsStorage interface
type LargeSegmentsStorageImpl struct {
	data      map[string][]string
	till      map[string]int64
	mutex     *sync.RWMutex
	tillMutex *sync.RWMutex
}

// NewLargeSegmentsStorage constructs a new LargeSegments cache
func NewLargeSegmentsStorage() *LargeSegmentsStorageImpl {
	return &LargeSegmentsStorageImpl{
		data:      make(map[string][]string),
		till:      make(map[string]int64),
		mutex:     &sync.RWMutex{},
		tillMutex: &sync.RWMutex{},
	}
}

// Count retuns the amount of Large Segments
func (s *LargeSegmentsStorageImpl) Count() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.data)
}

// SegmentsForUser returns the list of segments a certain user belongs to
func (s *LargeSegmentsStorageImpl) LargeSegmentsForUser(userKey string) []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	toReturn := make([]string, 0, len(s.data))
	for lsName, data := range s.data {
		i := sort.Search(len(data), func(i int) bool {
			return data[i] >= userKey
		})

		if i < len(data) && data[i] == userKey {
			toReturn = append(toReturn, lsName)
		}
	}

	return toReturn
}

// Update adds and remove keys to segments
func (s *LargeSegmentsStorageImpl) Update(name string, userKeys []string, till int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.data[name] = userKeys
	s.SetChangeNumber(name, till)
}

func (s *LargeSegmentsStorageImpl) SetChangeNumber(name string, till int64) {
	s.tillMutex.Lock()
	defer s.tillMutex.Unlock()
	s.till[name] = till
}

func (s *LargeSegmentsStorageImpl) ChangeNumber(name string) int64 {
	s.tillMutex.RLock()
	defer s.tillMutex.RUnlock()
	cn := s.till[name]
	if cn == 0 {
		cn = -1
	}
	return cn
}

var _ storage.LargeSegmentsStorage = (*LargeSegmentsStorageImpl)(nil)
