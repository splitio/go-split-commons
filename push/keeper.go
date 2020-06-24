package push

import (
	"strings"
	"sync"
)

const (
	prefix = "[?occupancy=metrics.publishers]"
)

type last struct {
	manager   string
	timestamp int64
}

// Keeper struct
type Keeper struct {
	managers          map[string]int
	activeRegion      string
	last              *last
	mutex             *sync.RWMutex
	runningPublishers chan int
}

// NewKeeper creates new keeper
func NewKeeper(runningPublishers chan int) *Keeper {
	return &Keeper{
		managers:          make(map[string]int),
		activeRegion:      "us-east-1",
		mutex:             &sync.RWMutex{},
		runningPublishers: runningPublishers,
	}
}

func (k *Keeper) cleanManagerPrefix(manager string) string {
	return strings.Replace(manager, prefix, "", -1)
}

// UpdateManagers updates current manager count
func (k *Keeper) UpdateManagers(manager string, publishers int) {
	defer k.mutex.Unlock()
	k.mutex.Lock()
	k.managers[k.cleanManagerPrefix(manager)] = publishers

	if manager == "control_pri" {
		k.runningPublishers <- publishers
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
