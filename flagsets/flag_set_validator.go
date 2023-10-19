package flagsets

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/exp/slices"
)

var (
	flagSetRegex = regexp.MustCompile("^[a-z0-9][_a-z0-9]{0,49}$")
)

func SanitizeMany(sets []string) ([]string, []error) {
	if len(sets) == 0 {
		return nil, nil
	}
	var sanitizedFlagSets []string
	var warnings []error
	for _, flagSet := range sets {
		sanitizedFlagSet, err := Sanitize(flagSet)
		warnings = append(warnings, err...)
		if sanitizedFlagSet != nil {
			if !slices.Contains(sanitizedFlagSets, *sanitizedFlagSet) {
				sanitizedFlagSets = append(sanitizedFlagSets, *sanitizedFlagSet)
				sort.Strings(sanitizedFlagSets)
			}
		}
	}
	return sanitizedFlagSets, warnings
}

func Sanitize(flagSet string) (*string, []error) {
	var warnings []error
	if lowerCased := strings.ToLower(flagSet); lowerCased != flagSet {
		warnings = append(warnings, fmt.Errorf(fmt.Sprintf("Flag Set name %s should be all lowercase - converting string to lowercase", flagSet)))
		flagSet = lowerCased
	}
	if trimmed := strings.TrimSpace(flagSet); trimmed != flagSet {
		warnings = append(warnings, fmt.Errorf(fmt.Sprintf("Flag Set name %s has extra whitespace, trimming", flagSet)))
		flagSet = trimmed
	}
	if !flagSetRegex.MatchString(flagSet) {
		warnings = append(warnings, fmt.Errorf(fmt.Sprintf("you passed %s, Flag Set must adhere to the regular expressions %s. This means a Flag Set must be "+
			"start with a letter, be in lowercase, alphanumeric and have a max length of 50 characters. %s was discarded.",
			flagSet, flagSetRegex, flagSet)))
		return nil, warnings
	}
	return &flagSet, warnings
}
