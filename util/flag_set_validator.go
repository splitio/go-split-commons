package util

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/splitio/go-toolkit/v5/logging"
	"golang.org/x/exp/slices"
)

var (
	flagSetRegex = regexp.MustCompile("^[a-z0-9][_a-z0-9]{0,49}$")
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
	if len(sets) == 0 {
		return nil, 0
	}
	invalidSets := 0
	var cleanFlagSets []string
	for _, flagSet := range sets {
		flagSetValid, valid := f.isFlagSetValid(flagSet)
		if valid {
			if !slices.Contains(cleanFlagSets, flagSetValid) {
				cleanFlagSets = append(cleanFlagSets, flagSetValid)
				sort.Strings(cleanFlagSets)
			}
		} else {
			invalidSets++
		}
	}
	return cleanFlagSets, invalidSets
}

func (f FlagSetValidator) AreValid(sets []string) ([]string, bool) {
	cleanSets, _ := f.Cleanup(sets)
	return cleanSets, len(cleanSets) != 0
}

func (f FlagSetValidator) isFlagSetValid(flagSet string) (string, bool) {
	if lowerCased := strings.ToLower(flagSet); lowerCased != flagSet {
		f.logger.Warning(fmt.Sprintf("Flag Set name %s should be all lowercase - converting string to lowercase", flagSet))
		flagSet = lowerCased
	}
	if trimmed := strings.TrimSpace(flagSet); trimmed != flagSet {
		f.logger.Warning(fmt.Sprintf("Flag Set name %s has extra whitespace, trimming", flagSet))
		flagSet = trimmed
	}
	if !flagSetRegex.MatchString(flagSet) {
		f.logger.Warning(fmt.Sprintf("you passed %s, Flag Set must adhere to the regular expressions %s. This means a Flag Set must be "+
			"start with a letter, be in lowercase, alphanumeric and have a max length of 50 characters. %s was discarded.",
			flagSet, flagSetRegex, flagSet))
		return "", false
	}
	return flagSet, true
}
