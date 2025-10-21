package redis

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/flagsets"
	"github.com/splitio/go-toolkit/v5/redis"
)

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

func cleanPrefixedKeys(keys []string, toRemove string) []string {
	toReturn := make([]string, 0, len(keys))

	for _, key := range keys {
		toReturn = append(toReturn, strings.Replace(key, toRemove, "", 1))
	}

	return toReturn
}
