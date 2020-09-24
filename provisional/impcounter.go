package provisional

import (
	"fmt"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/util"
)

// ImpressionsCounter struct for storing generated impressions counts
type ImpressionsCounter struct {
	impressionsCounts map[string]int64
	mutex             *sync.RWMutex
}

// NewImpressionsCounter creates new ImpressionsCounter
func NewImpressionsCounter() *ImpressionsCounter {
	return &ImpressionsCounter{
		impressionsCounts: make(map[string]int64),
		mutex:             &sync.RWMutex{},
	}
}

func makeKey(splitName string, timeFrame int64) string {
	return fmt.Sprintf("%s::%d", splitName, util.TruncateTimeFrame(timeFrame/int64(time.Millisecond)))
}

// Inc increments the quantity of impressions with the passed splitName and timeFrame
func (i *ImpressionsCounter) Inc(splitName string, timeFrame int64, amount int64) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	key := makeKey(splitName, timeFrame)
	currentAmount, _ := i.impressionsCounts[key]
	i.impressionsCounts[key] = currentAmount + amount
}

// PopAll returns all the elements stored in the cache and resets the cache
func (i *ImpressionsCounter) PopAll() map[string]int64 {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	toReturn := i.impressionsCounts
	i.impressionsCounts = make(map[string]int64)
	return toReturn
}

// Size returns how many keys are stored in cache
func (i *ImpressionsCounter) Size() int {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return len(i.impressionsCounts)
}
