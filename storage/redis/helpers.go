package redis

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
	"github.com/splitio/go-toolkit/v5/redis"
)

// calculateSets calculates the featureFlags that needs to be removed from sets and the featureFlags that needs to be
// added into the sets
func calculateSets(currentSets flagsets.FeaturesBySet, toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) (flagsets.FeaturesBySet, flagsets.FeaturesBySet) {
	// map[set]map[ff] for tracking all the sets that feature flags are going to be added
	setsToAdd := flagsets.NewFeaturesBySet(nil)

	for _, featureFlag := range toAdd {
		// for each feature flag, get all the sets that need to be added
		// map[set1][split1]{}
		// map[set1][split2]{}
		// map[set2][split1]{}
		for _, set := range featureFlag.Sets {
			setsToAdd.Add(set, featureFlag.Name)
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
	toRemoveSets := flagsets.Difference(currentSets, setsToAdd)
	toAddSets := flagsets.Difference(setsToAdd, currentSets)
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

func updateFlagSets(pipeline redis.Pipeline, toAdd flagsets.FeaturesBySet, toRemove flagsets.FeaturesBySet) {
	for _, set := range toAdd.Sets() {
		featureFlags := toAdd.FlagsFromSet(set)
		if len(featureFlags) == 0 {
			continue
		}
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for _, featureFlag := range featureFlags {
			featureFlagsNames = append(featureFlagsNames, featureFlag)
		}
		pipeline.SAdd(strings.Replace(KeyFlagSet, "{set}", set, 1), featureFlagsNames...)
	}
	for _, set := range toRemove.Sets() {
		featureFlags := toRemove.FlagsFromSet(set)
		if len(featureFlags) == 0 {
			continue
		}
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for _, featureFlag := range featureFlags {
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
