package specs

import "fmt"

const (
	FLAG_V1_0 = "1.0"
	FLAG_V1_1 = "1.1"
	FLAG_V1_2 = "1.2"
	FLAG_V1_3 = "1.3"
)

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(version string) *string {
	switch version {
	case FLAG_V1_0:
		return &version
	case FLAG_V1_1:
		return &version
	case FLAG_V1_2:
		return &version
	case FLAG_V1_3:
		return &version
	}

	return nil
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
