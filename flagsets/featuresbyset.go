package flagsets

import "github.com/splitio/go-split-commons/v7/dtos"

type FeaturesBySet struct {
	data map[string]map[string]struct{}
}

func NewFeaturesBySet(featureFlags []dtos.SplitDTO) FeaturesBySet {
	f := FeaturesBySet{data: make(map[string]map[string]struct{})}
	for _, featureFlag := range featureFlags {
		for _, set := range featureFlag.Sets {
			f.Add(set, featureFlag.Name)
		}
	}
	return f
}

func (f *FeaturesBySet) Add(set string, flag string) {
	curr, exists := f.data[set]
	if !exists {
		curr = make(map[string]struct{})
	}
	curr[flag] = struct{}{}
	f.data[set] = curr
}

func (f *FeaturesBySet) Sets() []string {
	sets := make([]string, 0, len(f.data))
	for set := range f.data {
		sets = append(sets, set)
	}
	return sets
}

func (f *FeaturesBySet) FlagsFromSet(set string) []string {
	flags := make([]string, 0)
	forSet, ok := f.data[set]
	if !ok {
		return flags
	}
	for flag := range forSet {
		flags = append(flags, flag)
	}
	return flags
}

func (f *FeaturesBySet) IsFlagInSet(set string, flag string) bool {
	forSet, ok := f.data[set]
	if !ok {
		return false
	}

	_, ok = forSet[flag]
	return ok
}

func (f *FeaturesBySet) RemoveFlagFromSet(set string, flag string) {
	forSet, ok := f.data[set]
	if !ok {
		return
	}
	delete(forSet, flag)
}

func Difference(one FeaturesBySet, two FeaturesBySet) FeaturesBySet {
	result := NewFeaturesBySet(nil)
	for set, featureFlags := range one.data {
		for featureFlagName := range featureFlags {
			isInSet := two.IsFlagInSet(set, featureFlagName)
			if !isInSet {
				result.Add(set, featureFlagName)
			}
		}
	}
	return result
}
