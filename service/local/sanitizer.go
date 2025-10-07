package local

import (
	"fmt"
	"math/rand"

	"github.com/splitio/go-split-commons/v7/dtos"
)

func splitSanitization(splitChange dtos.FFResponse) dtos.FFResponse {
	if splitChange.FFTill() < -1 {
		splitChange.SetFFTill(-1)
	}
	if splitChange.RBTill() < -1 {
		splitChange.SetRBTill(-1)
	}
	if splitChange.FFSince() < -1 || splitChange.FFSince() > splitChange.FFTill() {
		splitChange.SetFFSince(splitChange.FFTill())
	}
	if splitChange.RBSince() < -1 || splitChange.RBSince() > splitChange.RBTill() {
		splitChange.SetRBSince(splitChange.RBTill())
	}
	var splitResult []dtos.SplitDTO
	for i := 0; i < len(splitChange.FeatureFlags()); i++ {
		split := splitChange.FeatureFlags()[i]
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
			split.DefaultTreatment = "control"
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
	var ruleBasedSegmentResult []dtos.RuleBasedSegmentDTO
	for i := 0; i < len(splitChange.RuleBasedSegments()); i++ {
		ruleBased := splitChange.RuleBasedSegments()[i]
		if ruleBased.Name == "" {
			continue
		}
		if ruleBased.TrafficTypeName == "" {
			ruleBased.TrafficTypeName = "user"
		}
		if ruleBased.Status == "" || ruleBased.Status != "ARCHIVED" && ruleBased.Status != "ACTIVE" {
			ruleBased.Status = "ACTIVE"
		}
		if ruleBased.ChangeNumber < 0 {
			ruleBased.ChangeNumber = 0
		}
		ruleBasedSegmentResult = append(ruleBasedSegmentResult, ruleBased)
	}

	splitChange.ReplaceFF(splitResult)
	splitChange.ReplaceRB(ruleBasedSegmentResult)
	return splitChange
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
