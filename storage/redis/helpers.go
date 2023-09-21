package redis

import (
	"github.com/splitio/go-split-commons/v5/dtos"
)

type featuresBySet struct {
	data map[string]map[string]struct{}
}

func newFeaturesBySet(featureFlags []dtos.SplitDTO) featuresBySet {
	f := featuresBySet{data: make(map[string]map[string]struct{})}
	for _, featureFlag := range featureFlags {
		for _, set := range featureFlag.Sets {
			f.add(set, featureFlag.Name)
		}
	}
	return f
}

func (f *featuresBySet) add(set string, flag string) {
	curr, exists := f.data[set]
	if !exists {
		curr = make(map[string]struct{})
	}
	curr[flag] = struct{}{}
	f.data[set] = curr
}

func (f *featuresBySet) isFlagInSet(set string, flag string) bool {
	forSet, ok := f.data[set]
	if !ok {
		return false
	}

	_, ok = forSet[flag]
	return ok
}

func (f *featuresBySet) removeFlagFromSet(set string, flag string) {
	forSet, ok := f.data[set]
	if !ok {
		return
	}
	delete(forSet, flag)
}

func difference(one featuresBySet, two featuresBySet) featuresBySet {
	result := newFeaturesBySet([]dtos.SplitDTO{})
	for set, featureFlags := range one.data {
		for featureFlagName := range featureFlags {
			isInSet := two.isFlagInSet(set, featureFlagName)
			if !isInSet {
				result.add(set, featureFlagName)
			}
		}
	}
	return result
}

func removeDuplicates(one featuresBySet, two featuresBySet) featuresBySet {
	result := newFeaturesBySet([]dtos.SplitDTO{})
	for set, featureFlags := range one.data {
		for featureFlagName := range featureFlags {
			isInSet := two.isFlagInSet(set, featureFlagName)
			if !isInSet {
				result.add(set, featureFlagName)
			}
		}
	}
	return result
}

// calculateSets calculates the featureFlags that needs to be removed from sets and the featureFlags that needs to be
// added into the sets
func calculateSets(currentSets featuresBySet, toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) (featuresBySet, featuresBySet) {
	// map[set]map[ff] for tracking all the sets that feature flags are going to be added
	setsToAdd := newFeaturesBySet([]dtos.SplitDTO{})

	for _, featureFlag := range toAdd {
		// for each feature flag, get all the sets that need to be added
		// map[set1][split1]{}
		// map[set1][split2]{}
		// map[set2][split1]{}
		for _, set := range featureFlag.Sets {
			setsToAdd.add(set, featureFlag.Name)
		}
	}

	// at this point if the previous of set is compared against the added one
	// the sets which the feature flag needs to be added and removed can be calculated
	// previous version stored in redis of ff1 has sets: [setA, setB]
	// incomming sets of ff1 are: [setA, setC]
	// e.g:
	// currentSet map[ff1][setA], map[ff1][setB]
	// incommingSets map[setA][ff1], map[setC][ff1]
	// if setA is in previous version and in the incomming one the add is discarded
	// and setA is also removed from currentSet, which means that
	// the remaining items are going to be the ones to be removed
	// if setC is not in previous version, then is added into setsToAdd
	// the new version of ff1 is not linked to setB which is the only item
	// in currentSet
	toRemoveSets := difference(currentSets, setsToAdd)
	toAddSets := difference(setsToAdd, currentSets)
	return toAddSets, toRemoveSets
}
