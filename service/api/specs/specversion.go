package specs

import (
	"fmt"
	"slices"
)

const (
	FLAG_V1_0 = "1.0" // default
	FLAG_V1_1 = "1.1" // Semver
	FLAG_V1_2 = "1.2" // Large Segment
	FLAG_V1_3 = "1.3" // Rule-based Segment
)

var flagSpecs = []string{FLAG_V1_0, FLAG_V1_1, FLAG_V1_2, FLAG_V1_3}

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
