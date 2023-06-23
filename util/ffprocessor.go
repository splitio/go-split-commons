package util

import "github.com/splitio/go-split-commons/v4/dtos"

const (
	Active               = "ACTIVE"
	Archived             = "ARCHIVED"
	matcherTypeInSegment = "IN_SEGMENT"
)

// ProcessFeatureFlagChanges processes each split in the list and returns wether needs to be added or removed
func ProcessFeatureFlagChanges(featureFlags *dtos.SplitChangesDTO) ([]dtos.SplitDTO, []dtos.SplitDTO) {
	toRemove := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	toAdd := make([]dtos.SplitDTO, 0, len(featureFlags.Splits))
	for idx := range featureFlags.Splits {
		if featureFlags.Splits[idx].Status == Active {
			toAdd = append(toAdd, featureFlags.Splits[idx])
		} else {
			toRemove = append(toRemove, featureFlags.Splits[idx])
		}
	}
	return toAdd, toRemove
}

// AppendSegmentNames returns the list of segments referenced from split
func AppendSegmentNames(dst []string, splits *dtos.SplitChangesDTO) []string {
	for _, split := range splits.Splits {
		for _, cond := range split.Conditions {
			for _, matcher := range cond.MatcherGroup.Matchers {
				if matcher.MatcherType == matcherTypeInSegment && matcher.UserDefinedSegment != nil {
					dst = append(dst, matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
	}
	return dst
}
