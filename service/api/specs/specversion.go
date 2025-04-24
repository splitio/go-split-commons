package specs

import (
	"fmt"

	"golang.org/x/exp/slices"
)

const (
	FLAG_V1_0 = "1.0" // default
	FLAG_V1_1 = "1.1" // Semver Matcher
	FLAG_V1_2 = "1.2" // Large Segment Matcher
)

var flagSpecs = []string{FLAG_V1_0, FLAG_V1_1, FLAG_V1_2}
var Latest = flagSpecs[len(flagSpecs)-1]

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(version string) *string {
	ok := slices.Contains(flagSpecs, version)
	if !ok {
		return nil
	}

	return &version
}

func ParseAndValidate(spec string) (string, error) {
	if len(spec) == 0 {
		// return default flag spec
		return FLAG_V1_0, nil
	}

	if Match(spec) == nil {
		return spec, fmt.Errorf("unsupported flag spec: %s", spec)
	}

	return spec, nil
}
