package local

import (
	"fmt"
	"math/rand"

	"github.com/splitio/go-split-commons/v4/dtos"
)

func splitSanitization(splitChange dtos.SplitChangesDTO) *dtos.SplitChangesDTO {
	if splitChange.Till < -1 {
		splitChange.Till = -1
	}
	if splitChange.Since < -1 || splitChange.Since > splitChange.Till {
		splitChange.Since = splitChange.Till
	}
	var splitResult []dtos.SplitDTO
	for i := 0; i < len(splitChange.Splits); i++ {
		split := splitChange.Splits[i]
		if split.Name == "" {
			continue
		}
		if split.TrafficTypeName == "" {
			split.TrafficTypeName = "user"
		}
		if split.TrafficAllocation < 0 || split.TrafficAllocation > 100 {
			split.TrafficAllocation = 100
		}
		if split.TrafficAllocationSeed == 0 {
			split.TrafficAllocationSeed = -(rand.Int63n(9) + 1) * 1000
		}
		if split.Seed == 0 {
			split.Seed = -(rand.Int63n(9) + 1) * 1000
		}
		if split.Status == "" || split.Status != "ARCHIVED" && split.Status != "ACTIVE" {
			split.Status = "ACTIVE"
		}
		if split.DefaultTreatment == "" {
			split.DefaultTreatment = "on"
		}
		if split.ChangeNumber < 0 {
			split.ChangeNumber = 0
		}
		if split.Algo != 2 {
			split.Algo = 2
		}
		if len(split.Conditions) == 0 || split.Conditions[len(split.Conditions)-1].ConditionType != "ROLLOUT" ||
			len(split.Conditions[len(split.Conditions)-1].MatcherGroup.Matchers) == 0 || split.Conditions[len(split.Conditions)-1].MatcherGroup.Matchers[0].MatcherType != "ALL_KEYS" {
			condition := createRolloutCondition("off")
			condition.MatcherGroup.Matchers[0].KeySelector = &dtos.KeySelectorDTO{
				TrafficType: split.TrafficTypeName,
			}
			split.Conditions = append(split.Conditions, condition)
		}
		splitResult = append(splitResult, split)
	}
	splitChange.Splits = splitResult
	return &splitChange
}

func segmentSanitization(segmentChange dtos.SegmentChangesDTO, segmentName string) (*dtos.SegmentChangesDTO, error) {
	if segmentChange.Name == "" {
		return nil, fmt.Errorf("the %s segment dto doesn't have name", segmentName)
	}
	addedKeys := make(map[string]struct{}, len(segmentChange.Added))
	for _, key := range segmentChange.Added {
		addedKeys[key] = struct{}{}
	}
	removedKeys := make(map[string]struct{}, len(segmentChange.Removed))
	for _, key := range segmentChange.Removed {
		_, exists := addedKeys[key]
		if !exists {
			removedKeys[key] = struct{}{}
		}
	}
	newRemoved := make([]string, 0, len(removedKeys))
	for k := range removedKeys {
		newRemoved = append(newRemoved, k)
	}
	segmentChange.Removed = newRemoved

	if segmentChange.Till < -1 || segmentChange.Till == 0 {
		segmentChange.Till = -1
	}
	if segmentChange.Since < -1 || segmentChange.Since > segmentChange.Till {
		segmentChange.Since = segmentChange.Till
	}
	return &segmentChange, nil
}
