package flagsets

type FlagSetFilter struct {
	cfgFlagSets  map[string]struct{}
	shouldFilter bool
}

func NewFlagSetFilter(configFlagSets []string) FlagSetFilter {
	flagSets := make(map[string]struct{}, len(configFlagSets))
	for _, flagSet := range configFlagSets {
		flagSets[flagSet] = struct{}{}
	}
	return FlagSetFilter{
		cfgFlagSets:  flagSets,
		shouldFilter: len(flagSets) > 0,
	}
}

func (f *FlagSetFilter) IsPresent(flagSet string) bool {
	if !f.shouldFilter {
		return true
	}
	_, ok := f.cfgFlagSets[flagSet]
	return ok
}

func (f *FlagSetFilter) Instersect(flagSets []string) bool {
	if !f.shouldFilter {
		return true
	}
	for _, flagSet := range flagSets {
		isPresent := f.IsPresent(flagSet)
		if isPresent {
			return true
		}
	}
	return false
}
