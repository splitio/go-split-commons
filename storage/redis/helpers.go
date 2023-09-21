package redis

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-toolkit/v5/redis"
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
	result := newFeaturesBySet(nil)
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
	setsToAdd := newFeaturesBySet(nil)

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

func updateFeatureFlags(pipeline redis.Pipeline, toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) (map[string]error, []string) {
	failedToAdd := make(map[string]error)
	addedInPipe := make([]string, 0, len(toAdd))

	for _, split := range toAdd {
		keyToStore := strings.Replace(KeySplit, "{split}", split.Name, 1)
		raw, err := json.Marshal(split)
		if err != nil {
			failedToAdd[split.Name] = fmt.Errorf("failed to serialize split: %w", err)
			continue
		}

		// Attach each Feature Flag update into Redis Pipeline
		pipeline.Set(keyToStore, raw, 0)
		addedInPipe = append(addedInPipe, split.Name)
	}

	if len(toRemove) > 0 {
		toRemoveKeys := make([]string, 0, len(toRemove))
		for idx := range toRemove {
			toRemoveKeys = append(toRemoveKeys, strings.Replace(KeySplit, "{split}", toRemove[idx].Name, 1))
		}
		pipeline.Del(toRemoveKeys...)
	}

	return failedToAdd, addedInPipe
}

func updateFlagSets(pipeline redis.Pipeline, toAdd featuresBySet, toRemove featuresBySet) {
	for set, featureFlags := range toAdd.data {
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for featureFlag := range featureFlags {
			featureFlagsNames = append(featureFlagsNames, featureFlag)
		}
		pipeline.SAdd(strings.Replace(KeyFlagSet, "{set}", set, 1), featureFlagsNames...)
	}
	for set, featureFlags := range toRemove.data {
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for featureFlag := range featureFlags {
			featureFlagsNames = append(featureFlagsNames, featureFlag)
		}
		pipeline.SRem(strings.Replace(KeyFlagSet, "{set}", set, 1), featureFlagsNames...)
	}
}

func updateTrafficTypes(pipeline redis.Pipeline, currentFeatureFlags []dtos.SplitDTO, toAdd []dtos.SplitDTO) {
	for _, featureFlag := range currentFeatureFlags {
		pipeline.Decr(strings.Replace(KeyTrafficType, "{trafficType}", featureFlag.TrafficTypeName, 1))
	}

	for idx := range toAdd {
		pipeline.Incr(strings.Replace(KeyTrafficType, "{trafficType}", toAdd[idx].TrafficTypeName, 1))
	}
}
