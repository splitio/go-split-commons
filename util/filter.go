package util

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

func (f *FlagSetFilter) Match(flagSets []string) bool {
	if !f.shouldFilter {
		return true
	}
	for configFlagSet := range f.cfgFlagSets {
		for _, flagSet := range flagSets {
			if configFlagSet == flagSet {
				return true
			}
		}
	}
	return false
}
