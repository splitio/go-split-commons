package filter

import (
	"github.com/splitio/go-split-commons/v4/storage"
)

// Adapter description
type Adapter struct {
	filter storage.Filter
}

// NewAdapter description
func NewAdapter(f storage.Filter) storage.FilterAdpter {
	return &Adapter{
		filter: f,
	}
}

// Add description
func (a *Adapter) Add(featureName string, key string) {
	a.filter.Add(featureName + key)
}

// Contains description
func (a *Adapter) Contains(featureName string, key string) bool {
	return a.filter.Contains(featureName + key)
}

// Clear description
func (a *Adapter) Clear() {
	a.filter.Clear()
}
