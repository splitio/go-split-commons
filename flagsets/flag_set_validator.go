package flagsets

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/splitio/go-split-commons/v9/dtos"
	"golang.org/x/exp/slices"
)

var (
	flagSetRegex = regexp.MustCompile("^[a-z0-9][_a-z0-9]{0,49}$")
)

// SanitizeMany sanitizes flagsets provided and returns the list of errors
func SanitizeMany(sets []string) ([]string, []error) {
	if len(sets) == 0 {
		return []string{}, nil
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

// Sanitize sanitizes flagset provided and returns the list of errors
func Sanitize(flagSet string) (*string, []error) {
	var warnings []error
	if lowerCased := strings.ToLower(flagSet); lowerCased != flagSet {
		warnings = append(warnings, dtos.FlagSetValidatonError{
			Message: fmt.Sprintf("Flag Set name %s should be all lowercase - converting string to lowercase", flagSet),
		})
		flagSet = lowerCased
	}

	if trimmed := strings.TrimSpace(flagSet); trimmed != flagSet {
		warnings = append(warnings, dtos.FlagSetValidatonError{
			Message: fmt.Sprintf("Flag Set name %s has extra whitespace, trimming", flagSet),
		})
		flagSet = trimmed
	}
	if !flagSetRegex.MatchString(flagSet) {
		warnings = append(warnings, dtos.FlagSetValidatonError{
			Message: fmt.Sprintf("you passed %s, Flag Set must adhere to the regular expressions %s. This means a Flag Set must "+
				"start with a letter or number, be in lowercase, alphanumeric and have a max length of 50 characters. %s was discarded.",
				flagSet, flagSetRegex, flagSet),
		})
		return nil, warnings
	}
	return &flagSet, warnings
}
