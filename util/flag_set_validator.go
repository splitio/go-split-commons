package util

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/splitio/go-toolkit/v5/logging"
	"golang.org/x/exp/slices"
)

const (
	flagSetRegex = "^[a-z0-9][_a-z0-9]{0,49}$"
)

type FlagSetValidator struct {
	logger logging.LoggerInterface
}

func NewFlagSetValidator(logger logging.LoggerInterface) *FlagSetValidator {
	return &FlagSetValidator{
		logger: logger,
	}
}

func (f FlagSetValidator) Cleanup(sets []string) ([]string, int) {
	regex := regexp.MustCompile(flagSetRegex)
	if len(sets) == 0 {
		return []string{}, 0
	}
	invalidSets := 0
	var cleanFlagSets = []string{}
	for i := 0; i < len(sets); i++ {
		var flagSet = sets[i]
		if flagSet != strings.ToLower(flagSet) {
			f.logger.Warning(fmt.Sprintf("Flag Set name %s should be all lowercase - converting string to lowercase", flagSet))
			flagSet = strings.ToLower(flagSet)
		}
		if strings.TrimSpace(flagSet) != flagSet {
			f.logger.Warning(fmt.Sprintf("Flag Set name %s has extra whitespace, trimming", flagSet))
			flagSet = strings.TrimSpace(flagSet)
		}
		match := regex.MatchString(flagSet)
		if !match {
			invalidSets++
			f.logger.Warning(fmt.Sprintf("you passed %s, Flag Set must adhere to the regular expressions %s. This means a Flag Set must be "+
				"start with a letter, be in lowercase, alphanumeric and have a max length of 50 characters. %s was discarded.",
				flagSet, flagSetRegex, flagSet))
			continue
		}
		if !slices.Contains(cleanFlagSets, flagSet) {
			cleanFlagSets = append(cleanFlagSets, flagSet)
		}
		sort.Strings(cleanFlagSets)
	}
	return cleanFlagSets, invalidSets
}

func (f FlagSetValidator) AreValid(sets []string) ([]string, bool) {
	cleanSets, _ := f.Cleanup(sets)
	return cleanSets, len(cleanSets) != 0
}
