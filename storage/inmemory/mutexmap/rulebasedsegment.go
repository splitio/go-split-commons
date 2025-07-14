package mutexmap

import (
	"sync"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
)

// RuleBasedSegmentsStorageImpl implements the RuleBasedSegmentsStorage interface
type RuleBasedSegmentsStorageImpl struct {
	data      map[string]dtos.RuleBasedSegment
	till      int64
	mutex     *sync.RWMutex
	tillMutex *sync.RWMutex
}

// NewRuleBasedSegmentsStorage constructs a new RuleBasedSegments cache
func NewRuleBasedSegmentsStorage() *RuleBasedSegmentsStorageImpl {
	return &RuleBasedSegmentsStorageImpl{
		data:      make(map[string]dtos.RuleBasedSegment),
		till:      -1,
		mutex:     &sync.RWMutex{},
		tillMutex: &sync.RWMutex{},
	}
}

// Update atomically registers new rule-based, removes archived ones and updates the change number
func (r *RuleBasedSegmentsStorageImpl) Update(toAdd []dtos.RuleBasedSegment, toRemove []dtos.RuleBasedSegment, till int64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, ruleBased := range toAdd {
		r.data[ruleBased.Name] = ruleBased
	}

	for _, ruleBased := range toRemove {
		_, exists := r.data[ruleBased.Name]
		if exists {
			delete(r.data, ruleBased.Name)
		}
	}
	r.SetChangeNumber(till)
}

// SetChangeNumber sets the till value belong to rule-based
func (r *RuleBasedSegmentsStorageImpl) SetChangeNumber(till int64) error {
	r.tillMutex.Lock()
	defer r.tillMutex.Unlock()
	r.till = till
	return nil
}

// ChangeNumber return the actual rule-based till
func (r *RuleBasedSegmentsStorageImpl) ChangeNumber() int64 {
	return r.till
}

// All returns a list with a copy of each rule-based.
// NOTE: This method will block any further operations regarding rule-baseds. Use with caution
func (r *RuleBasedSegmentsStorageImpl) All() []dtos.RuleBasedSegment {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	ruleBasedList := make([]dtos.RuleBasedSegment, 0)
	for _, ruleBased := range r.data {
		ruleBasedList = append(ruleBasedList, ruleBased)
	}
	return ruleBasedList
}

// RuleBasedSegmentNames returns a slice with the names of all the current rule-baseds
func (r *RuleBasedSegmentsStorageImpl) RuleBasedSegmentNames() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	ruleBasedNames := make([]string, 0)
	for key := range r.data {
		ruleBasedNames = append(ruleBasedNames, key)
	}
	return ruleBasedNames
}

// SegmentNames returns a slice with the names of all segments referenced in rule-based
func (r *RuleBasedSegmentsStorageImpl) GetSegments() *set.ThreadUnsafeSet {
	segments := set.NewSet()

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	for _, ruleBased := range r.data {
		for _, condition := range ruleBased.Conditions {
			for _, matcher := range condition.MatcherGroup.Matchers {
				if matcher.UserDefinedSegment != nil {
					segments.Add(matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
		for _, excluded := range ruleBased.Excluded.Segments {
			if excluded.Type == dtos.TypeStandard {
				segments.Add(excluded.Name)
			}
		}
	}
	return segments
}

// Contains returns true or false if all the rule-based segment names are present
func (r *RuleBasedSegmentsStorageImpl) Contains(ruleBasedSegmentNames []string) bool {
	if len(ruleBasedSegmentNames) == 0 {
		return false
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	for _, name := range ruleBasedSegmentNames {
		_, exists := r.data[name]
		if !exists {
			return false
		}
	}
	return true
}

func (r *RuleBasedSegmentsStorageImpl) Clear() {
	r.tillMutex.RLock()
	defer r.tillMutex.RUnlock()
	r.till = -1

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	r.data = make(map[string]dtos.RuleBasedSegment)
}
