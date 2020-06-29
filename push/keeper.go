package push

import (
	"strings"
	"sync"
)

const (
	// PublisherNotPresent there are no publishers sending data
	PublisherNotPresent = iota
	// PublisherAvailable there are publishers running
	PublisherAvailable
	prefix = "[?occupancy=metrics.publishers]"
)

type last struct {
	manager   string
	timestamp int64
}

// Keeper struct
type Keeper struct {
	managers     map[string]int
	activeRegion string
	last         *last
	publishers   chan int
	mutex        *sync.RWMutex
}

// NewKeeper creates new keeper
func NewKeeper(publishers chan int) *Keeper {
	return &Keeper{
		managers:     make(map[string]int),
		activeRegion: "us-east-1",
		mutex:        &sync.RWMutex{},
		publishers:   publishers,
	}
}

func (k *Keeper) cleanManagerPrefix(manager string) string {
	return strings.Replace(manager, prefix, "", -1)
}

// UpdateManagers updates current manager count
func (k *Keeper) UpdateManagers(manager string, publishers int) {
	defer k.mutex.Unlock()
	parsedManager := k.cleanManagerPrefix(manager)
	k.mutex.Lock()
	k.managers[parsedManager] = publishers

	if parsedManager == "control_pri" {
		if publishers <= 0 {
			k.publishers <- PublisherNotPresent
			return
		}
		k.publishers <- PublisherAvailable
	}
}

// UpdateLastNotification updates last message received
func (k *Keeper) UpdateLastNotification(manager string, timestamp int64) {
	defer k.mutex.Unlock()
	k.mutex.Lock()
	k.last = &last{
		manager:   k.cleanManagerPrefix(manager),
		timestamp: timestamp,
	}
}
