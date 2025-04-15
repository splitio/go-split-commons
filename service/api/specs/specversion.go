package specs

import (
	"fmt"

	"golang.org/x/exp/slices"
)

var FlagSpecs = []string{
	"1.0", // default
	"1.1", // Semver Matcher
	"1.2", // Large Segment Matcher
}

var Latest = string(FlagSpecs[len(FlagSpecs)-1])
var Default = string(FlagSpecs[0])

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(version string) *string {
	ok := slices.Contains(FlagSpecs, version)
	if !ok {
		return nil
	}

	return &version
}

func ParseAndValidate(spec string) (string, error) {
	if len(spec) == 0 {
		// return default flag spec
		return Default, nil
	}

	if Match(spec) == nil {
		return spec, fmt.Errorf("unsupported flag spec: %s", spec)
	}

	return spec, nil
}
